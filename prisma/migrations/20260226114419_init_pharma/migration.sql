-- CreateEnum
CREATE TYPE "Source" AS ENUM ('NETMEDS', 'PHARMEASY', 'ONEMG', 'TRUEMEDS');

-- CreateEnum
CREATE TYPE "SearchStatus" AS ENUM ('PENDING', 'APPROVED', 'REJECTED');

-- CreateTable
CREATE TABLE "Medicine" (
    "id" SERIAL NOT NULL,
    "brand" TEXT NOT NULL,
    "strength" TEXT NOT NULL,
    "form" TEXT NOT NULL,
    "variant" TEXT NOT NULL,
    "canonicalName" TEXT NOT NULL,
    "approved" BOOLEAN NOT NULL DEFAULT false,
    "createdAt" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT "Medicine_pkey" PRIMARY KEY ("id")
);

-- CreateTable
CREATE TABLE "Product" (
    "id" SERIAL NOT NULL,
    "medicineId" INTEGER NOT NULL,
    "source" "Source" NOT NULL,
    "name" TEXT NOT NULL,
    "pack" TEXT,
    "price" TEXT,
    "originalPrice" TEXT,
    "discount" TEXT,
    "productUrl" TEXT,
    "endpoint" TEXT,
    "scrapedAt" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT "Product_pkey" PRIMARY KEY ("id")
);

-- CreateTable
CREATE TABLE "SearchRequest" (
    "id" SERIAL NOT NULL,
    "query" TEXT NOT NULL,
    "brand" TEXT,
    "strength" TEXT,
    "form" TEXT,
    "variant" TEXT,
    "status" "SearchStatus" NOT NULL DEFAULT 'PENDING',
    "createdAt" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT "SearchRequest_pkey" PRIMARY KEY ("id")
);

-- CreateIndex
CREATE UNIQUE INDEX "Medicine_canonicalName_key" ON "Medicine"("canonicalName");

-- CreateIndex
CREATE UNIQUE INDEX "Product_medicineId_source_key" ON "Product"("medicineId", "source");

-- AddForeignKey
ALTER TABLE "Product" ADD CONSTRAINT "Product_medicineId_fkey" FOREIGN KEY ("medicineId") REFERENCES "Medicine"("id") ON DELETE RESTRICT ON UPDATE CASCADE;
