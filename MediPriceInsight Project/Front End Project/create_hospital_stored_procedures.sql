-- Drop and create all stored procedures for hospital_dataset

-- 1. Get distinct cities
DROP FUNCTION IF EXISTS get_distinct_cities();
CREATE OR REPLACE FUNCTION get_distinct_cities()
RETURNS TABLE (city text)
AS $$
BEGIN
    RETURN QUERY
    SELECT DISTINCT h."city"::text
    FROM public.hospital_dataset h
    WHERE h."city" IS NOT NULL
    ORDER BY h."city"::text;
END;
$$ LANGUAGE plpgsql;

-- 2. Get distinct regions
DROP FUNCTION IF EXISTS get_distinct_regions();
CREATE OR REPLACE FUNCTION get_distinct_regions()
RETURNS TABLE (region text)
AS $$
BEGIN
    RETURN QUERY
    SELECT DISTINCT h."region"::text
    FROM public.hospital_dataset h
    WHERE h."region" IS NOT NULL
    ORDER BY h."region"::text;
END;
$$ LANGUAGE plpgsql;

-- 3. Get cities for a specific region
DROP FUNCTION IF EXISTS get_cities_for_region(text);
CREATE OR REPLACE FUNCTION get_cities_for_region(p_region text)
RETURNS TABLE (city text)
AS $$
BEGIN
    RETURN QUERY
    SELECT DISTINCT h."city"::text
    FROM public.hospital_dataset h
    WHERE h."region" = p_region AND h."city" IS NOT NULL
    ORDER BY h."city"::text;
END;
$$ LANGUAGE plpgsql;

-- 4. Get codes and descriptions
DROP FUNCTION IF EXISTS get_codes_and_descriptions();
CREATE OR REPLACE FUNCTION get_codes_and_descriptions()
RETURNS TABLE (code text, description text)
AS $$
BEGIN
    RETURN QUERY
    SELECT DISTINCT h."code"::text, h."description"::text
    FROM public.hospital_dataset h
    ORDER BY h."code"::text;
END;
$$ LANGUAGE plpgsql;

-- 5. Search codes and descriptions
DROP FUNCTION IF EXISTS search_codes_and_descriptions(text);
CREATE OR REPLACE FUNCTION search_codes_and_descriptions(p_search_term text)
RETURNS TABLE (code text, description text)
AS $$
BEGIN
    RETURN QUERY
    SELECT DISTINCT h."code"::text, h."description"::text
    FROM public.hospital_dataset h
    WHERE h."code" ILIKE '%' || p_search_term || '%'
       OR h."description" ILIKE '%' || p_search_term || '%'
    ORDER BY h."code"::text
    LIMIT 10;
END;
$$ LANGUAGE plpgsql;

-- 6. Get hospital data by code, city, region
DROP FUNCTION IF EXISTS get_hospital_data(text, text, text);
CREATE OR REPLACE FUNCTION get_hospital_data(
    p_code text,
    p_city text DEFAULT NULL,
    p_region text DEFAULT NULL
)
RETURNS TABLE (
    code text,
    description text,
    city text,
    region text,
    hospital_name text,
    price numeric
)
AS $$
BEGIN
    RETURN QUERY
    SELECT
        h."code"::text,
        h."description"::text,
        h."city"::text,
        h."region"::text,
        h."hospital_name"::text,
        h."price"
    FROM public.hospital_dataset h
    WHERE h."code" = p_code
    AND (p_city IS NULL OR h."city" = p_city)
    AND (p_region IS NULL OR h."region" = p_region)
    ORDER BY h."hospital_name"::text;
END;
$$ LANGUAGE plpgsql;

-- 7. Get distinct payer names
DROP FUNCTION IF EXISTS get_distinct_payer_names();
CREATE OR REPLACE FUNCTION get_distinct_payer_names()
RETURNS TABLE (payer_name text)
AS $$
BEGIN
    RETURN QUERY
    SELECT DISTINCT h.payer_name::text
    FROM public.hospital_dataset h
    WHERE h.payer_name IS NOT NULL
    ORDER BY h.payer_name::text;
END;
$$ LANGUAGE plpgsql;

-- 8. Get distinct plan names
DROP FUNCTION IF EXISTS get_distinct_plan_names();
CREATE OR REPLACE FUNCTION get_distinct_plan_names()
RETURNS TABLE (plan_name text)
AS $$
BEGIN
    RETURN QUERY
    SELECT DISTINCT h.plan_name::text
    FROM public.hospital_dataset h
    WHERE h.plan_name IS NOT NULL
    ORDER BY h.plan_name::text;
END;
$$ LANGUAGE plpgsql; 