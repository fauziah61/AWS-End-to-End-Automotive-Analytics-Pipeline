--
-- PostgreSQL database dump
--

\restrict KuWN1OGyWNGkcJNynIWLVGKUxs2kHukvgzhptLalZtdBVTIENK9pVQQYhEw1AXf

-- Dumped from database version 18.3
-- Dumped by pg_dump version 18.4

-- Started on 2026-10-23 17:33:15

SET statement_timeout = 0;
SET lock_timeout = 0;
SET idle_in_transaction_session_timeout = 0;
SET transaction_timeout = 0;
SET client_encoding = 'UTF8';
SET standard_conforming_strings = on;
SELECT pg_catalog.set_config('search_path', '', false);
SET check_function_bodies = false;
SET xmloption = content;
SET client_min_messages = warning;
SET row_security = off;

SET default_tablespace = '';

SET default_table_access_method = heap;

--
-- TOC entry 221 (class 1259 OID 16506)
-- Name: stg_after_sales_raw; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.stg_after_sales_raw (
    service_ticket character varying(10),
    vin character varying(50),
    customer_id character varying(255),
    model character varying(50),
    service_date character varying(50),
    service_type character varying(5),
    created_at character varying(50)
);


--
-- TOC entry 219 (class 1259 OID 16491)
-- Name: stg_customers_raw; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.stg_customers_raw (
    id character varying(255) NOT NULL,
    name character varying(255),
    dob character varying(50),
    created_at character varying(50)
);


--
-- TOC entry 220 (class 1259 OID 16499)
-- Name: stg_sales_raw; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.stg_sales_raw (
    vin character varying(25),
    customer_id character varying(255),
    model character varying(50),
    invoice_date character varying(50),
    price character varying(255),
    created_at character varying(50)
);


--
-- TOC entry 4324 (class 0 OID 16506)
-- Dependencies: 221
-- Data for Name: stg_after_sales_raw; Type: TABLE DATA; Schema: public; Owner: -
--

COPY public.stg_after_sales_raw (service_ticket, vin, customer_id, model, service_date, service_type, created_at) FROM stdin;
T124-kgu1	MAS8160POE	3	RANGGO	2025-07-11	BP	2025-07-11 09:24:40.012
T560-jga1	JLK1368KDE	4	INNAVO	2025-08-04	PM	2025-08-04 10:12:54.003
T521-oai8	POI1059IIK	5	RAIZA	2026-09-10	GR	2026-09-10 12:45:02.391
\.


--
-- TOC entry 4322 (class 0 OID 16491)
-- Dependencies: 219
-- Data for Name: stg_customers_raw; Type: TABLE DATA; Schema: public; Owner: -
--

COPY public.stg_customers_raw (id, name, dob, created_at) FROM stdin;
1	Antonio	1998-08-04	2025-03-01 14:24:40.012
2	Brandon	2001-04-21	2025-03-02 08:12:54.003
3	Charlie	1980/11/15	2025-03-02 11:20:02.391
4	Dominikus	14/01/1995	2025-03-03 09:50:41.852
5	Erik	1900-01-01	2025-03-03 17:22:03.198
6	PT Black Bird	\N	2025-03-04 12:52:16.122
\.


--
-- TOC entry 4323 (class 0 OID 16499)
-- Dependencies: 220
-- Data for Name: stg_sales_raw; Type: TABLE DATA; Schema: public; Owner: -
--

COPY public.stg_sales_raw (vin, customer_id, model, invoice_date, price, created_at) FROM stdin;
JIS8135SAD	1	RAIZA	2025-03-01	350.000.000	2025-03-01 14:24:40.012
MAS8160POE	3	RANGGO	2025-05-19	430.000.000	2025-05-19 14:29:21.003
JLK1368KDE	4	INNAVO	2025-05-22	600.000.000	2025-05-22 16:10:28.120
JLK1869KDF	6	VELOS	2025-08-02	390.000.000	2025-08-02 14:04:31.021
JLK1962KOP	6	VELOS	2025-08-02	390.000.000	2025-08-02 15:21:04.201
\.


--
-- TOC entry 4170 (class 2606 OID 16498)
-- Name: stg_customers_raw stg_customers_raw_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.stg_customers_raw
    ADD CONSTRAINT stg_customers_raw_pkey PRIMARY KEY (id);


--
-- TOC entry 4173 (class 1259 OID 16510)
-- Name: idx_stg_after_sales_raw_customer_id; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_stg_after_sales_raw_customer_id ON public.stg_after_sales_raw USING btree (customer_id);


--
-- TOC entry 4174 (class 1259 OID 16509)
-- Name: idx_stg_after_sales_raw_vin; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_stg_after_sales_raw_vin ON public.stg_after_sales_raw USING btree (vin);


--
-- TOC entry 4171 (class 1259 OID 16504)
-- Name: idx_stg_sales_raw_customer_id; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_stg_sales_raw_customer_id ON public.stg_sales_raw USING btree (customer_id);


--
-- TOC entry 4172 (class 1259 OID 16505)
-- Name: idx_stg_sales_raw_vin; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_stg_sales_raw_vin ON public.stg_sales_raw USING btree (vin);


-- Completed on 2026-10-23 17:33:31

--
-- PostgreSQL database dump complete
--

\unrestrict KuWN1OGyWNGkcJNynIWLVGKUxs2kHukvgzhptLalZtdBVTIENK9pVQQYhEw1AXf

