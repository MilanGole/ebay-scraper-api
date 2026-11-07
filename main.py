import os
import re
import aiohttp
from urllib.parse import urlencode, urlparse, parse_qs
from parsel import Selector
from dotenv import load_dotenv

from fastapi import FastAPI, HTTPException, Query
from starlette.concurrency import run_in_threadpool
from sqlalchemy import create_engine, Column, Integer, String, Text, DateTime, func
from sqlalchemy.orm import declarative_base, sessionmaker
from sqlalchemy.dialects.postgresql import insert

# 1. Load Environment Variables
load_dotenv()

DATABASE_URL = os.getenv("DATABASE_URL")
SCRAPERAPI_KEY = os.getenv("SCRAPERAPI_KEY")

if not DATABASE_URL:
    raise RuntimeError("DATABASE_URL is missing in .env file")

# 2. Database Connection Setup (SQLAlchemy)
engine = create_engine(DATABASE_URL)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
Base = declarative_base()

# 3. Define SQL Table Schema inside Python
class EbayItem(Base):
    __tablename__ = "ebay_items"

    id = Column(Integer, primary_key=True, index=True)
    ebay_item_number = Column(String(50), unique=True, nullable=False)
    title = Column(Text, nullable=False)
    condition = Column(String(100))
    price = Column(String(50))
    url = Column(Text, nullable=False)
    updated_at = Column(DateTime, server_default=func.now(), onupdate=func.now())

# Make sure tables exist in Supabase
Base.metadata.create_all(bind=engine)

# 4. FastAPI Setup
app = FastAPI(title="eBay Scraper API + Supabase Postgres", version="2.1")


def extract_item_id(url: str) -> str:
    """Extracts eBay item ID from standard (/itm/) and catalog (/p/?iid=) URLs."""
    if not url:
        return "N/A"

    itm_match = re.search(r"/itm/(\d+)", url)
    if itm_match:
        return itm_match.group(1)

    parsed_url = urlparse(url)
    query_params = parse_qs(parsed_url.query)
    if "iid" in query_params:
        return query_params["iid"][0]

    return "N/A"


def save_to_supabase(scraped_data: list[dict]):
    """Upserts (Insert or Update on duplicate) items into Supabase PostgreSQL."""
    db = SessionLocal()
    try:
        for item in scraped_data:
            stmt = insert(EbayItem).values(
                ebay_item_number=item["ebay_item_number"],
                title=item["title"],
                condition=item["condition"],
                price=item["price"],
                url=item["url"]
            ).on_conflict_do_update(
                index_elements=['ebay_item_number'],
                set_={
                    "title": item["title"],
                    "price": item["price"],
                    "condition": item["condition"],
                    "updated_at": func.now()
                }
            )
            db.execute(stmt)
        db.commit()
        print(f"✅ Successfully saved/updated {len(scraped_data)} records in Supabase PostgreSQL!")
    except Exception as e:
        db.rollback()
        print(f"❌ Database error: {e}")
        raise HTTPException(status_code=500, detail=f"Database Insertion Error: {str(e)}")
    finally:
        db.close()


async def run_ebay_scraper(target_url: str, limit: int = 10) -> list[dict]:
    """Scrapes eBay URL and returns structured items."""
    params = {"api_key": SCRAPERAPI_KEY, "url": target_url, "render": "false"}
    proxy_url = f"https://api.scraperapi.com/?{urlencode(params)}"

    timeout = aiohttp.ClientTimeout(total=60)
    async with aiohttp.ClientSession(timeout=timeout) as session:
        async with session.get(proxy_url) as response:
            if response.status != 200:
                raise HTTPException(
                    status_code=response.status, 
                    detail=f"ScraperAPI failed with status code {response.status}"
                )

            html = await response.text()
            selector = Selector(text=html)

            items = selector.css(
                "li.brwrvr__item-card, li.bsig__item, ul.b-list__items_nofooter > li, li.s-item"
            )

            scraped_data = []
            seen_urls = set()

            for item in items:
                if len(scraped_data) >= limit:
                    break

                url = item.css("a.bsig__title__wrapper::attr(href), a.s-item__link::attr(href)").get()
                if not url or url in seen_urls:
                    continue

                item_id = extract_item_id(url)
                if item_id == "N/A":
                    continue  # Prevents unique constraint violation crashes on null/invalid item IDs

                title_parts = item.css("h3.bsig__title__text ::text, .s-item__title ::text").getall()
                full_title = " ".join([t.strip() for t in title_parts if t.strip()])
                clean_title = full_title.replace("New listing", "").strip()

                if not clean_title or "Shop on eBay" in clean_title:
                    continue

                condition = item.css(".bsig__listingCondition span::text, .s-item__subtitle ::text").get()
                clean_condition = condition.strip() if condition else "N/A"

                price = item.css(".bsig__price--displayprice::text, .s-item__price::text").get()
                clean_price = price.strip() if price else "N/A"

                seen_urls.add(url)
                scraped_data.append(
                    {
                        "ebay_item_number": item_id,
                        "title": clean_title,
                        "condition": clean_condition,
                        "price": clean_price,
                        "url": url,
                    }
                )

            return scraped_data


@app.get("/")
def home():
    return {"status": "online", "docs": "/docs"}


@app.get("/scrape")
async def scrape_endpoint(
    target_url: str = Query(..., description="The eBay category or search page URL"),
    limit: int = Query(10, ge=1, le=50, description="Number of items to return")
):
    # 1. Scrape Data asynchronously
    scraped_data = await run_ebay_scraper(target_url, limit)
    
    if not scraped_data:
        return {"status": "warning", "message": "No items matched selectors", "data": []}

    # 2. Save Data safely in a background threadpool to avoid blocking FastAPI's event loop
    await run_in_threadpool(save_to_supabase, scraped_data)

    return {
        "status": "success",
        "saved_to_database": True,
        "total_items": len(scraped_data),
        "data": scraped_data
    }