DROP TABLE public."NHM_Import";

CREATE TABLE public."NHM_Import"
(
    subgenus character varying COLLATE pg_catalog."default",
    family character varying COLLATE pg_catalog."default",
    materialPrimaryTypeNumber character varying COLLATE pg_catalog."default",
    kindOfMaterial character varying COLLATE pg_catalog."default",
    phylum character varying COLLATE pg_catalog."default",
    GUID uuid,
    materialTypes character varying COLLATE pg_catalog."default",
    media character varying COLLATE pg_catalog."default",
    multimedia character varying COLLATE pg_catalog."default",
    kindOfMedia character varying COLLATE pg_catalog."default",
    materialCount character varying COLLATE pg_catalog."default",
    taxonRank character varying COLLATE pg_catalog."default",
    specificEpithet character varying COLLATE pg_catalog."default",
    imageCategory character varying COLLATE pg_catalog."default",
    kingdom character varying COLLATE pg_catalog."default",
    suborder character varying COLLATE pg_catalog."default",
    material character varying COLLATE pg_catalog."default",
    infraspecificEpithet character varying COLLATE pg_catalog."default",
    subfamily character varying COLLATE pg_catalog."default",
    class character varying COLLATE pg_catalog."default",
    superfamily character varying COLLATE pg_catalog."default",
    genus character varying COLLATE pg_catalog."default",
    created date,
    type character varying COLLATE pg_catalog."default",
    modified date,
    british character varying COLLATE pg_catalog."default",
    materialSex character varying COLLATE pg_catalog."default",
    scientificName character varying COLLATE pg_catalog."default",
    materialStage character varying COLLATE pg_catalog."default",
    _id bigint,
    currentScientificName character varying COLLATE pg_catalog."default",
    "order" character varying COLLATE pg_catalog."default"
)
WITH (
    OIDS = FALSE
)
TABLESPACE pg_default;

ALTER TABLE public."NHM_Import"
    OWNER to postgres;