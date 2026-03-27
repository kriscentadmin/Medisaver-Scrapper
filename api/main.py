import asyncio
import os
import subprocess
import sys

from fastapi import FastAPI, HTTPException
from prisma import Prisma
from core.detector import detect_medicine_parts
from datetime import UTC, datetime
from fastapi.middleware.cors import CORSMiddleware
from prisma.enums import Source
from pydantic import BaseModel
from typing import Optional
from utils.medicine_parser import parse_medicine
import runner as scraper_runner
from pathlib import Path
app = FastAPI()
db = Prisma()
scraper_process: subprocess.Popen | None = None


def scraper_timestamp() -> str:
    return scraper_runner.date_time_iso()


class UpdateProductSchema(BaseModel):
    name: Optional[str]
    pack: Optional[str]
    price: Optional[str]
    originalPrice: Optional[str]
    discount: Optional[str]
    productUrl: Optional[str]
    endpoint: Optional[str]

class OperatorMedicineRequest(BaseModel):
    name: str
    operatorName: str

class AddProductSchema(BaseModel):
    medicineId: int
    source: Source
    name: str
    pack: Optional[str] = None
    price: Optional[str] = None
    originalPrice: Optional[str] = None
    discount: Optional[str] = None
    productUrl: Optional[str] = None
    endpoint: Optional[str] = None




app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        origin.strip()
        for origin in os.getenv(
            "CORS_ORIGINS",
            "http://localhost:3000,http://192.168.29.162:3000,https://medisaver-scrapper-admin.vercel.app",
        ).split(",")
        if origin.strip()
    ],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.on_event("startup")
async def startup():
    await db.connect()
    await scraper_runner.ensure_runner_tables()
    await scraper_runner.recover_interrupted_run()

@app.on_event("shutdown")
async def shutdown():
    await db.disconnect()


# ================= USER SEARCH =================

@app.get("/autocomplete")
async def autocomplete(q: str):
    q = q.strip().upper()

    if len(q) < 2:
        return []

    medicines = await db.medicine.find_many(
        where={
            "OR": [
                {"brand": {"startsWith": q}},
                {"canonicalName": {"contains": q}},
            ],
            # "approved": True,
        },
        take=8,
    )

    return [
        {
            "id": m.id,
            "canonicalName": m.canonicalName,
            "brand": m.brand,
            "strength": m.strength,
            "form": m.form,
            "variant": m.variant,
        }
        for m in medicines
    ]




# ================User Click On Suggestioned by mediicne Id==============
@app.get("/search/by-id")
async def search_by_medicine_id(medicineId: int):
    med = await db.medicine.find_unique(
        where={"id": medicineId},
        include={"products": True},
    )

    if not med:
        return {
            "status": "NOT_FOUND",
            "message": "Medicine does not exist"
        }

    return {
        "status": "FOUND",
        "medicine": {
            "id": med.id,
            "canonicalName": med.canonicalName,
            "brand": med.brand,
            "strength": med.strength,
            "form": med.form,
            "variant": med.variant,
            "approved":med.approved
        },
        "products": med.products,   # comparison data
    }



# //////////////////////////////////////////
# ==================== Operator Request ============================
@app.post("/operator/request-medicine")
async def operator_request_medicine(payload: OperatorMedicineRequest):

    parts = await parse_medicine(payload.name)

    try:
        request = await db.searchrequest.create(
            data={
                **parts,
                "operatorName": payload.operatorName
            }
        )

        return {
            "status": "REQUEST_CREATED",
            "data": request
        }

    except:
        exists = await db.searchrequest.find_first(
            where={
                "brand": parts["brand"],
                "strength": parts["strength"],
                "form": parts["form"],
                "variant": parts["variant"]
            }
        )

        return {
            "status": "ALREADY_REQUESTED",
            "data": exists
        }
    

    # ======================== GET ALL SEARCH REQUESTS ========================

@app.get("/admin/search-requests")
async def get_search_requests():

    requests = await db.searchrequest.find_many(
        order={"id": "desc"}
    )

    return {
        "total": len(requests),
        "data": requests
    }

    
# /////////////////////////////////////////////////////////////////
# ======================== Add Data into Medicine Collection ====================

@app.post("/admin/create-medicine-from-request/{requestId}")
async def create_medicine_from_request(requestId: int):

    req = await db.searchrequest.find_unique(
        where={"id": requestId}
    )

    if not req:
        raise HTTPException(404, "Search request not found")

    # check if medicine already exists
    med = await db.medicine.find_first(
        where={
            "brand": req.brand,
            "strength": req.strength,
            "form": req.form,
            "variant": req.variant
        }
    )

    if med:
        return {
            "status": "MEDICINE_ALREADY_EXISTS",
            "medicineId": med.id
        }

    med = await db.medicine.create(
        data={
            "brand": req.brand,
            "strength": req.strength,
            "form": req.form,
            "variant": req.variant,
            "canonicalName": req.canonicalName,
            "approved": True
        }
    )

    # delete request
    await db.searchrequest.delete(
        where={"id": requestId}
    )

    return {
        "status": "MEDICINE_CREATED",
        "medicineId": med.id
    }


# /////////////////////////////////////////////////////////////////
# ======================== Delete Search Request Data By Admin ===========================

@app.delete("/admin/search-request/{requestId}")
async def delete_search_request(requestId: int):

    request = await db.searchrequest.find_unique(
        where={"id": requestId}
    )

    if not request:
        raise HTTPException(404, "Request not found")

    await db.searchrequest.delete(
        where={"id": requestId}
    )

    return {
        "status": "DELETED"
    }

# ================= ADMIN =================

@app.post("/admin/approve-medicine")
async def approve_medicine(payload: dict):
    med = await db.medicine.upsert(
        where={"canonicalName": payload["canonicalName"]},
        data={
            "create": {
                **payload,
                "approved": True
            },
            "update": {
                "approved": True
            }
        }
    )

    await db.searchrequest.update_many(
        where={"canonicalName": payload["canonicalName"]},
        data={"status": "APPROVED"}
    )

    return med

# ///////////////////////////////////////////////////
# =====================Add Medicine Data using medicinId by the Admin ========================

@app.post("/admin/add-product")
async def add_product(payload: AddProductSchema):

    # check medicine exists
    med = await db.medicine.find_unique(
        where={"id": payload.medicineId}
    )

    if not med:
        raise HTTPException(404, "Medicine not found")

    product = await db.product.upsert(
        where={
            "medicineId_source": {
                "medicineId": payload.medicineId,
                "source": payload.source
            }
        },
        data={
            "create": {
                "medicineId": payload.medicineId,
                "source": payload.source,
                "name": payload.name,
                "pack": payload.pack,
                "price": payload.price,
                "originalPrice": payload.originalPrice,
                "discount": payload.discount,
                "productUrl": payload.productUrl,
                "endpoint": payload.endpoint
            },
            "update": {
                "name": payload.name,
                "pack": payload.pack,
                "price": payload.price,
                "originalPrice": payload.originalPrice,
                "discount": payload.discount,
                "productUrl": payload.productUrl,
                "endpoint": payload.endpoint,
                "scrapedAt": datetime.now(UTC)
            }
        }
    )

    return {
        "status": "PRODUCT_SAVED",
        "product": product
    }


# ///////////////////////////////////////////////////////////
# =============Get All Medicine Data ========================
@app.get("/medicine-data")
async def get_medicine_with_products(
    page: int = 1,
    limit: int = 20
):
    skip = (page-1)*limit

    medicines = await db.medicine.find_many(
        skip = skip,
        take = limit,
        include={"products":True},
        order={"id":"asc"},
    )

    total_count = await db.medicine.count()

    return {
        "page":page,
        "limit":limit,
        "total":total_count,
        "totalPages":(total_count+limit-1) // limit,
        "data":medicines
    }


# /////////////////////////////////////////////////
# ================Delete Medicine From Products ===================
@app.delete("/admin/product")
async def delete_product(medicineId: int, source: Source):

    med = await db.medicine.find_unique(
        where={"id": medicineId}
    )

    if not med:
        raise HTTPException(404, "Medicine not found")

    try:
        await db.product.delete(
            where={
                "medicineId_source": {
                    "medicineId": medicineId,
                    "source": source
                }
            }
        )
    except:
        raise HTTPException(404, "Product from this source not found")

    return {"status": "DELETED"}

# ////////////////////////////////////////////////////////
# ====================Update Medicine In Products======================

@app.put("/admin/product/{productId}")
async def update_product(productId: int, payload: UpdateProductSchema):

    product = await db.product.find_unique(
        where={"id": productId}
    )

    if not product:
        raise HTTPException(404, "Product not found")

    updated = await db.product.update(
        where={"id": productId},
        data=payload.dict(exclude_unset=True)
    )

    return {
        "status": "UPDATED",
        "product": updated
    }



# =============== API To Start Scrapper =====================

@app.api_route("/admin/scraper/start", methods=["GET", "POST"])
@app.api_route("/admin/scarper/start", methods=["GET", "POST"])
async def start_scraper():
    global scraper_process

    if scraper_process and scraper_process.poll() is None:
        return {
            "status": "ALREADY_RUNNING",
            "scraper": await scraper_runner.get_runner_status(),
            "progress": await scraper_runner.get_progress_snapshot(),
        }

    started_at = scraper_timestamp()
    await scraper_runner.update_run_status(
        running=True,
        started_at=started_at,
        finished_at=None,
        summary={"status": "starting", "startedAt": started_at},
        error=None,
    )

    scraper_process = subprocess.Popen(
        [sys.executable, "-m", "runner"],
        cwd=str(Path(__file__).resolve().parent.parent),
    )

    return {
        "status": "STARTED",
        "scraper": await scraper_runner.get_runner_status(),
        "progress": await scraper_runner.get_progress_snapshot(),
    }

# =============== API To Check Status =====================


@app.get("/admin/scraper/status")
async def get_scraper_status():
    return {
        "scraper": await scraper_runner.get_runner_status(),
        "progress": await scraper_runner.get_progress_snapshot(),
    }

# =============== API To Stop Scrapper =====================

@app.api_route("/admin/scraper/stop", methods=["GET", "POST"])
@app.api_route("/admin/scarper/stop", methods=["GET", "POST"])
async def stop_scraper():
    global scraper_process

    if not scraper_process or scraper_process.poll() is not None:
        return {
            "status": "NOT_RUNNING",
            "scraper": await scraper_runner.get_runner_status(),
            "progress": await scraper_runner.get_progress_snapshot(),
        }

    try:
        scraper_process.terminate()
        scraper_process.wait(timeout=10)
    except Exception:
        scraper_process.kill()
        scraper_process.wait(timeout=10)

    stopped_at = scraper_timestamp()
    await scraper_runner.update_run_status(
        running=False,
        finished_at=stopped_at,
        summary={"status": "stopped", "finishedAt": stopped_at},
        error="stopped by admin",
    )
    scraper_process = None

    return {
        "status": "STOPPED",
        "scraper": await scraper_runner.get_runner_status(),
        "progress": await scraper_runner.get_progress_snapshot(),
    }
