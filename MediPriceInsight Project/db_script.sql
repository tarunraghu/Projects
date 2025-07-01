-- Create the healthcarepoc database if it doesn't exist
CREATE DATABASE IF NOT EXISTS healthcarepoc;

--
-- PostgreSQL database dump
--

-- Dumped from database version 16.9
-- Dumped by pg_dump version 16.9

SET statement_timeout = 0;
SET lock_timeout = 0;
SET idle_in_transaction_session_timeout = 0;
SET client_encoding = 'UTF8';
SET standard_conforming_strings = on;
SELECT pg_catalog.set_config('search_path', '', false);
SET check_function_bodies = false;
SET xmloption = content;
SET client_min_messages = warning;
SET row_security = off;

--
-- Name: clean_hospital_charges(); Type: PROCEDURE; Schema: public; Owner: postgres
--

CREATE PROCEDURE public.clean_hospital_charges()
    LANGUAGE plpgsql
    AS $$
BEGIN
    -- Drop the temp_max_id_hospital_name table if it exists
    DROP TABLE IF EXISTS temp_max_id_hospital_name;

    -- Get the max_id_hospital_name from hospital_charges
    CREATE TEMP TABLE temp_max_id_hospital_name AS
    SELECT hospital_name
    FROM public.hospital_charges where id = (select max(id) from public.hospital_charges);

    -- Update hospital name in hospital_charges
    UPDATE public.hospital_charges
    SET hospital_name = 'East Los Angeles Doctors Hospital'
    WHERE hospital_name = 'Pipeline';

    -- Update hospital name in hospital_address
    UPDATE public.hospital_address
    SET hospital_name = 'East Los Angeles Doctors Hospital'
    WHERE hospital_name = 'Pipeline';

    -- Drop the temp_hospital_charges table if it exists
    DROP TABLE IF EXISTS temp_hospital_charges;

    -- Create a temporary table for hospital charges
    CREATE TEMP TABLE temp_hospital_charges AS
    SELECT *
    FROM public.hospital_charges
    WHERE hospital_name IN (SELECT hospital_name FROM temp_max_id_hospital_name);

    -- Perform cleanup of numeric columns on temp_hospital_charges
    UPDATE temp_hospital_charges
    SET standard_charge_min = NULLIF(standard_charge_min, 999999999),
        standard_charge_max = NULLIF(standard_charge_max, 999999999),
        standard_charge_negotiated_dollar = NULLIF(standard_charge_negotiated_dollar, 999999999),
        standard_charge_discounted_cash = NULLIF(standard_charge_discounted_cash, 999999999),
        standard_charge_gross = NULLIF(standard_charge_gross, 999999999),
        estimated_amount = NULLIF(estimated_amount, 999999999);

    -- Drop the temp_descriptions table if it exists
    DROP TABLE IF EXISTS temp_descriptions;

    -- Create a temporary table for descriptions
    CREATE TEMP TABLE temp_descriptions AS
    SELECT code, MIN(description) AS description
    FROM public.hospital_charges
    GROUP BY code;

    -- Delete existing records in hospital_dataset for max_id_hospital_name
    DELETE FROM public.hospital_dataset
    WHERE hospital_name IN (SELECT hospital_name FROM temp_max_id_hospital_name);

    -- Insert cleaned data into public.hospital_dataset
    INSERT INTO public.hospital_dataset (
        id,
        hospital_name,
        code,
        code_type,
        payer_name,
        plan_name,
        standard_charge_min,
        standard_charge_max,
        standard_charge_gross,
        standard_charge_negotiated_dollar,
        is_active,
        created_at,
        updated_at,
        hospital_address,
        description,
        region,
        city
    )
    SELECT 
        hc.id,
        hc.hospital_name,
        td.code,
        hc.code_type,
        COALESCE(NULLIF(TRIM(hc.payer_name), ''), hc.hospital_name) AS payer_name,
        COALESCE(NULLIF(TRIM(hc.plan_name), ''), hc.hospital_name) AS plan_name,
        COALESCE(hc.standard_charge_min, hc.standard_charge_max, hc.standard_charge_negotiated_dollar, hc.standard_charge_discounted_cash, hc.standard_charge_gross, hc.estimated_amount) AS standard_charge_min,
        COALESCE(hc.standard_charge_max, hc.standard_charge_min, hc.standard_charge_negotiated_dollar, hc.standard_charge_discounted_cash, hc.standard_charge_gross, hc.estimated_amount) AS standard_charge_max,
        COALESCE(
            hc.standard_charge_gross,
            hc.standard_charge_discounted_cash,
            hc.estimated_amount,
            hc.standard_charge_min,
            hc.standard_charge_max,
            hc.standard_charge_negotiated_dollar
        ) AS standard_charge_gross,
        COALESCE(
            hc.standard_charge_negotiated_dollar,
            hc.standard_charge_discounted_cash,
            hc.estimated_amount,
            hc.standard_charge_min,
            hc.standard_charge_max,
            hc.standard_charge_gross
        ) AS standard_charge_negotiated_dollar,
        hc.is_active,
        hc.created_at,
        hc.updated_at,
        b.hospital_address,
        upper(td.description) as description,
        b.region as region,
        b.city as city
    FROM temp_hospital_charges hc
    LEFT JOIN hospital_address b ON hc.hospital_name = b.hospital_name::text
    JOIN temp_descriptions td ON hc.code = td.code
    WHERE 
        hc.standard_charge_min IS NOT NULL OR
        hc.standard_charge_max IS NOT NULL OR
        hc.standard_charge_negotiated_dollar IS NOT NULL OR
        hc.standard_charge_discounted_cash IS NOT NULL OR
        hc.standard_charge_gross IS NOT NULL OR
        hc.estimated_amount IS NOT NULL;

	UPDATE public.hospital_dataset AS h
	SET description = sub.min_description
	FROM (
    SELECT code, MIN(description) AS min_description
    FROM public.hospital_dataset
    GROUP BY code
	) AS sub
	WHERE h.code = sub.code;
	END;
	
$$;


ALTER PROCEDURE public.clean_hospital_charges() OWNER TO postgres;

SET default_tablespace = '';

SET default_table_access_method = heap;

--
-- Name: hospital_address; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.hospital_address (
    id integer NOT NULL,
    hospital_name character varying(255),
    last_updated_on character varying(50),
    version character varying(50),
    hospital_location character varying(255),
    hospital_address character varying(255),
    region character varying(255),
    city character varying(255)
);


ALTER TABLE public.hospital_address OWNER TO postgres;

--
-- Name: hospital_address_id_seq; Type: SEQUENCE; Schema: public; Owner: postgres
--

CREATE SEQUENCE public.hospital_address_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER SEQUENCE public.hospital_address_id_seq OWNER TO postgres;

--
-- Name: hospital_address_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: postgres
--

ALTER SEQUENCE public.hospital_address_id_seq OWNED BY public.hospital_address.id;


--
-- Name: hospital_charges; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.hospital_charges (
    id integer NOT NULL,
    hospital_name text,
    description text,
    code character varying(50),
    code_type character varying(50),
    payer_name character varying(255),
    plan_name character varying(255),
    standard_charge_gross numeric(20,2),
    standard_charge_discounted_cash numeric(20,2),
    standard_charge_negotiated_dollar numeric(20,2),
    standard_charge_min numeric(20,2),
    standard_charge_max numeric(20,2),
    estimated_amount numeric(20,2),
    is_active boolean DEFAULT true,
    created_at timestamp without time zone DEFAULT CURRENT_TIMESTAMP,
    updated_at timestamp without time zone DEFAULT CURRENT_TIMESTAMP
);


ALTER TABLE public.hospital_charges OWNER TO postgres;

--
-- Name: hospital_charges_archive; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.hospital_charges_archive (
    id integer NOT NULL,
    hospital_name text,
    description text,
    code character varying(50),
    code_type character varying(50),
    payer_name character varying(255),
    plan_name character varying(255),
    standard_charge_gross numeric(20,2),
    standard_charge_negotiated_dollar numeric(20,2),
    standard_charge_min numeric(20,2),
    standard_charge_max numeric(20,2),
    standard_charge_discounted_cash numeric(20,2),
    estimated_amount numeric(20,2),
    original_created_at timestamp without time zone,
    archive_reason text,
    archived_at timestamp without time zone DEFAULT CURRENT_TIMESTAMP
);


ALTER TABLE public.hospital_charges_archive OWNER TO postgres;

--
-- Name: hospital_charges_archive_id_seq; Type: SEQUENCE; Schema: public; Owner: postgres
--

CREATE SEQUENCE public.hospital_charges_archive_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER SEQUENCE public.hospital_charges_archive_id_seq OWNER TO postgres;

--
-- Name: hospital_charges_archive_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: postgres
--

ALTER SEQUENCE public.hospital_charges_archive_id_seq OWNED BY public.hospital_charges_archive.id;


--
-- Name: hospital_charges_id_seq; Type: SEQUENCE; Schema: public; Owner: postgres
--

CREATE SEQUENCE public.hospital_charges_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER SEQUENCE public.hospital_charges_id_seq OWNER TO postgres;

--
-- Name: hospital_charges_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: postgres
--

ALTER SEQUENCE public.hospital_charges_id_seq OWNED BY public.hospital_charges.id;


--
-- Name: hospital_dataset; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.hospital_dataset (
    id integer,
    hospital_name text,
    code character varying(50),
    code_type character varying(50),
    payer_name text,
    plan_name text,
    standard_charge_min numeric(20,2),
    standard_charge_max numeric(20,2),
    standard_charge_gross numeric(20,2),
    standard_charge_negotiated_dollar numeric(20,2),
    is_active boolean,
    created_at timestamp without time zone,
    updated_at timestamp without time zone,
    hospital_address character varying(255),
    description text,
    region character varying(255),
    city character varying(255)
);


ALTER TABLE public.hospital_dataset OWNER TO postgres;

--
-- Name: hospital_log; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.hospital_log (
    id integer NOT NULL,
    hospital_name text,
    user_name character varying(255),
    ingestion_type character varying(50),
    ingestion_start_time timestamp without time zone,
    ingestion_end_time timestamp without time zone,
    total_records integer,
    unique_records integer,
    archived_records integer,
    status character varying(50),
    error_message text,
    file_path text,
    processing_time_seconds numeric,
    created_at timestamp without time zone DEFAULT CURRENT_TIMESTAMP,
    updated_at timestamp without time zone DEFAULT CURRENT_TIMESTAMP
);


ALTER TABLE public.hospital_log OWNER TO postgres;

--
-- Name: hospital_log_id_seq; Type: SEQUENCE; Schema: public; Owner: postgres
--

CREATE SEQUENCE public.hospital_log_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER SEQUENCE public.hospital_log_id_seq OWNER TO postgres;

--
-- Name: hospital_log_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: postgres
--

ALTER SEQUENCE public.hospital_log_id_seq OWNED BY public.hospital_log.id;


--
-- Name: hospital_address id; Type: DEFAULT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.hospital_address ALTER COLUMN id SET DEFAULT nextval('public.hospital_address_id_seq'::regclass);


--
-- Name: hospital_charges id; Type: DEFAULT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.hospital_charges ALTER COLUMN id SET DEFAULT nextval('public.hospital_charges_id_seq'::regclass);


--
-- Name: hospital_charges_archive id; Type: DEFAULT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.hospital_charges_archive ALTER COLUMN id SET DEFAULT nextval('public.hospital_charges_archive_id_seq'::regclass);


--
-- Name: hospital_log id; Type: DEFAULT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.hospital_log ALTER COLUMN id SET DEFAULT nextval('public.hospital_log_id_seq'::regclass);


--
-- Name: hospital_address hospital_address_hospital_name_key; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.hospital_address
    ADD CONSTRAINT hospital_address_hospital_name_key UNIQUE (hospital_name);


--
-- Name: hospital_address hospital_address_pkey; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.hospital_address
    ADD CONSTRAINT hospital_address_pkey PRIMARY KEY (id);


--
-- Name: hospital_charges_archive hospital_charges_archive_pkey; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.hospital_charges_archive
    ADD CONSTRAINT hospital_charges_archive_pkey PRIMARY KEY (id);


--
-- Name: hospital_charges hospital_charges_hospital_name_description_code_code_type_p_key; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.hospital_charges
    ADD CONSTRAINT hospital_charges_hospital_name_description_code_code_type_p_key UNIQUE (hospital_name, description, code, code_type, payer_name, plan_name);


--
-- Name: hospital_charges hospital_charges_pkey; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.hospital_charges
    ADD CONSTRAINT hospital_charges_pkey PRIMARY KEY (id);


--
-- Name: hospital_log hospital_log_pkey; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.hospital_log
    ADD CONSTRAINT hospital_log_pkey PRIMARY KEY (id);


--
-- Name: idx_hospital_charges_hospital_name; Type: INDEX; Schema: public; Owner: postgres
--

CREATE INDEX idx_hospital_charges_hospital_name ON public.hospital_charges USING btree (hospital_name);


--
-- PostgreSQL database dump complete
--

