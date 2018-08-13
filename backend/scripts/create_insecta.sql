DROP TABLE IF EXISTS "insecta_taxon";
DROP TABLE IF EXISTS "insecta_distribution";
DROP TABLE IF EXISTS "insecta_description";
DROP TABLE IF EXISTS "insecta_vernacular";
DROP TABLE IF EXISTS "insecta_reference";

CREATE TABLE "insecta_taxon" (
  "insectaID" uuid default uuid_generate_v1mc(),
  "taxonID" int NOT NULL,
  "identifier" varchar(255) default NULL,
  "datasetID" varchar(255) default NULL,
  "datasetName" varchar(255) default NULL,
  "acceptedNameUsageID" int default NULL,
  "parentNameUsageID" int default NULL,
  "taxonomicStatus" varchar(255) default NULL,
  "taxonRank" varchar(255) default NULL,
  "verbatimTaxonRank" varchar(255) default NULL,
  "scientificName" varchar(255) default NULL,
  "kingdom" varchar(255) default NULL,
  "phylum" varchar(255) default NULL,
  "class" varchar(255) default NULL,
  "order" varchar(255) default NULL,
  "superfamily" varchar(255) default NULL,
  "family" varchar(255) default NULL,
  "genericName" varchar(255) default NULL,
  "genus" varchar(255) default NULL,
  "subgenus" varchar(255) default NULL,
  "specificEpithet" varchar(255) default NULL,
  "infraspecificEpithet" varchar(255) default NULL,
  "scientificNameAuthorship" varchar(255) default NULL,
  "source" text,
  "namePublishedIn" text,
  "nameAccordingTo" varchar(255) default NULL,
  "modified" varchar(255) default NULL,
  "description" text,
  "taxonConceptID" varchar(255) default NULL,
  "scientificNameID" varchar(255) default NULL,
  "references" varchar(255) default NULL,
  "isExtinct" varchar(10) default NULL,
  "sourceTable" varchar(255),
  "dateCreate" date,
  "dateUpdate" date,
  PRIMARY KEY  ("insectaID")
);

CREATE TABLE "insecta_distribution" (
  "taxonID" int NOT NULL,
  "locationID" varchar(255) default NULL,
  "locality" text,
  "occurrenceStatus" varchar(255) default NULL,
  "establishmentMeans" varchar(255) default NULL
);

CREATE TABLE "insecta_description" (
  "taxonID" int NOT NULL,
  "locality" text
);

CREATE TABLE "insecta_reference" (
  "taxonID" int NOT NULL,
  "creator" varchar(255) default NULL,
  "date" varchar(255) default NULL,
  "title" varchar(255) default NULL,
  "description" text,
  "identifier" varchar(255) default NULL,
  "type" varchar(255) default NULL
);

CREATE TABLE "insecta_vernacular" (
  "taxonID" int NOT NULL,
  "vernacularName" varchar(255) NULL,
  "language" varchar(255) NULL,
  "countryCode" varchar(255) NULL,
  "locality" varchar(255) NULL,
  "transliteration" varchar(255) NULL
);

CREATE INDEX "PK_insecta" ON "insecta_taxon" USING btree ("insectaID" ASC)  TABLESPACE pg_default;
CREATE INDEX "ItaxonID" ON "insecta_taxon" USING btree ("taxonID" ASC)  TABLESPACE pg_default;
CREATE INDEX "IdistaxonID" ON "insecta_distribution" USING btree ("taxonID" ASC)  TABLESPACE pg_default;
CREATE INDEX "IdestaxonID" ON "insecta_description" USING btree ("taxonID" ASC)  TABLESPACE pg_default;
CREATE INDEX "IreftaxonID" ON "insecta_reference" USING btree ("taxonID" ASC)  TABLESPACE pg_default;
CREATE INDEX "IvertaxonID" ON "insecta_vernacular" USING btree ("taxonID"ASC)  TABLESPACE pg_default;
