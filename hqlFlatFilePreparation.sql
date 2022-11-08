/*****************************************************************************/
/****Script Version 0.1 / This is a DRAFT ************************************/
/*****************************************************************************/


--create a base of loyalty cards
DROP   TABLE IF     EXISTS bdap_pilot_analysis.prj_cluster_samplecards;
CREATE TABLE IF NOT EXISTS bdap_pilot_analysis.prj_cluster_samplecards
STORED AS ORC 
AS  
SELECT	b.loyaltycardnumber 
, COUNT(b.bon_id) AS cnt_bons
,MIN(bon_day) AS min_bon_day
,MAX(bon_day) AS max_bon_day
FROM bdap_pilot_cleaned.src_loyalty_bridge b
WHERE b.mandant_id = 2 
AND b.bon_day BETWEEN 20201001 AND 20210128	
GROUP BY b.loyaltycardnumber
;

--create a base of loyalty cards
DROP   TABLE IF     EXISTS bdap_pilot_analysis.prj_cluster_base_bons;
CREATE TABLE IF NOT EXISTS bdap_pilot_analysis.prj_cluster_base_bons
STORED AS ORC 
AS  
SELECT 
t.*
FROM (
	SELECT b.* 
	FROM bdap_pilot_cleaned.src_loyalty_bridge b
	INNER JOIN bdap_pilot_analysis.prj_cluster_samplecards s  --filter join 
	ON b.loyaltycardnumber = s.loyaltycardnumber
	WHERE b.mandant_id = 2 
	AND b.bon_day BETWEEN 20201001 AND 20210128	 
	ORDER BY RAND()   -- random sample
)t 
LIMIT 100000 ;  --limit results for demo purposes

--checks
SELECT * FROM bdap_pilot_analysis.prj_cluster_samplecards;
SELECT * FROM bdap_pilot_analysis.prj_cluster_base_bons;
SELECT count(*) FROM bdap_pilot_analysis.prj_cluster_samplecards;
SELECT count(*) FROM bdap_pilot_analysis.prj_cluster_base_bons;


--create a bon-level subset of ondata, enriched with loyaltycard number
DROP   TABLE IF     EXISTS bdap_pilot_analysis.prj_cluster_base_bons_detail;
CREATE TABLE IF NOT EXISTS bdap_pilot_analysis.prj_cluster_base_bons_detail
STORED AS ORC 
AS  
SELECT 
 v.bon_id  --bon identifier
,v.bon_zeile --row of bon
,v.kl_art_id --article / item id
,v.ean_kasse --ean code
,v.wg_id --article group / merchandise group
,v.menge --qty by position
,v.einh_id --measure 
,v.umsatz_brutto --turnover + vat
,v.mwst_id --vat_id
,v.aktion_id --promotion (1/0)
,v.wup_id --market (warenumschlagsplatz)
,v.talon_id --weight articles
,v.start_bon_zeile --timestamp first 
,v.k_bon_dat --head: date of receipt
,v.k_bon_nr --head: running number of receipts
,v.k_kunde_fg --head: counts as customer
,v.k_bon_beginn --head: timestamp begin
,v.k_bon_ende --head:timestamp  end
,v.k_bon_booking --head: timestamp booking 
,v.k_dat_booking --head: date booking
,v.k_turnover_fg --head: 1 Counts for turnover calculation / 0 exclude
,v.k_wrg_id --head: currency id
,v.mandant_id --country
,v.bon_day --integer date of bon
,b.loyaltycardnumber
FROM bdap_pilot_cleaned.src_teradata_bondata_sales v 		--receipt data --> bons
INNER JOIN bdap_pilot_analysis.prj_cluster_base_bons b
ON  v.mandant_id = b.mandant_id
AND v.wup_id = b.wup_id
AND v.bon_id = b.bon_id
LEFT JOIN bdap_pilot_cleaned.src_teradata_bondata_storno o  --filtering on cancelled receipts --> Storno
ON 		(v.bon_id=o.bon_id   --join
AND 	v.bon_day=o.bon_day  --join
AND 	o.mandant_id = 2       --join optimization
AND 	o.bon_day  BETWEEN 20201001 AND 20210128) --join optimization / limit storno data
WHERE 	v.bon_day  BETWEEN 20201001 AND 20210128 --select a timeframe / limit primary data
AND   	v.k_turnover_fg = 1					--turnover relevance
AND  	v.wg_id < 980 						--exlude deposit / redemption --> pfand
AND	  	o.bon_id  IS NULL					--no receipts from the cancellation table --> storno bons
AND   	v.mandant_id = 2						--country
AND   	v.k_kunde_fg = 1 						--receipt counts as customer
AND   	v.k_kasse_nr between 1 AND 40		--specific points of sale
;
--checks
SELECT * 
FROM bdap_pilot_analysis.prj_cluster_base_bons_detail 
WHERE wup_id = 6010 AND k_bon_dat = '2020-11-07' 
ORDER BY mandant_id,bon_id,bon_zeile;
SELECT COUNT(*) FROM bdap_pilot_analysis.prj_cluster_base_bons_detail;
 


--prepare data to measure distance in between to receipts
DROP   TABLE IF     EXISTS bdap_pilot_analysis.prj_cluster_bons_with_date;
CREATE TABLE IF NOT EXISTS bdap_pilot_analysis.prj_cluster_bons_with_date
STORED AS ORC 
AS  
SELECT 
 t.*
,ROW_NUMBER()OVER(PARTITION BY t.mandant_id , t.loyaltycardnumber ORDER BY t.k_bon_dat) AS Line_num
FROM (
SELECT 
  d.mandant_id, d.loyaltycardnumber, d.bon_id, d.k_bon_dat
FROM bdap_pilot_analysis.prj_cluster_base_bons_detail d
WHERE mandant_id = 2 
GROUP BY
  d.mandant_id, d.loyaltycardnumber, d.bon_id, d.k_bon_dat
ORDER BY 
  d.mandant_id, d.loyaltycardnumber, d.bon_id, d.k_bon_dat
)t
;
--measure distance in between to receipts by join the predeceding one
DROP   TABLE IF     EXISTS bdap_pilot_analysis.prj_cluster_bons_with_date_shift;
CREATE TABLE IF NOT EXISTS bdap_pilot_analysis.prj_cluster_bons_with_date_shift
STORED AS ORC 
AS  
SELECT a.mandant_id	,a.loyaltycardnumber	,a.bon_id	,a.k_bon_dat,a.line_num,b.k_bon_dat AS shift_k_bon_dat,b.line_num AS shift_line_num
,DATEDIFF(b.k_bon_dat,a.k_bon_dat) AS days_between_receipt
FROM bdap_pilot_analysis.prj_cluster_bons_with_date a
LEFT JOIN bdap_pilot_analysis.prj_cluster_bons_with_date b
ON  a.mandant_id = b.mandant_id
AND a.loyaltycardnumber = b.loyaltycardnumber
AND a.line_num = (b.line_num-1) --get the predeceding receipt
;
-- aggregation bon_row to loyalty_card with KPI
-- Demographics (Age / Gender)
-- 

DROP   TABLE IF     EXISTS bdap_pilot_analysis.prj_cluster_bons_with_card_and_date; --flatfile!
CREATE TABLE IF NOT EXISTS bdap_pilot_analysis.prj_cluster_bons_with_card_and_date
STORED AS ORC 
AS
SELECT 
 d.mandant_id
,d.loyaltycardnumber
,COUNT(DISTINCT d.wup_id)    AS cnt_distinct_wup_id
,COUNT(DISTINCT d.wg_id)     AS cnt_distinct_wg_id
,COUNT(DISTINCT d.bon_id)    AS cnt_distinct_bon_id
,COUNT(DISTINCT d.kl_art_id) AS cnt_distinct_kl_art_id
,SUM(CASE WHEN aktion_id IS NULL THEN 1 ELSE 0 END) AS cnt_items_in_promo
,SUM(d.menge) AS sum_menge
,SUM(d.umsatz_brutto) AS sum_umsatz_brutto
,SUM(d.menge) / COUNT(DISTINCT d.bon_id)  AS avg_menge
,SUM(d.umsatz_brutto) / COUNT(DISTINCT d.bon_id)  AS avg_umsatz_brutto
,AVG(COALESCE(s.days_between_receipt,0)) AS avg_number_of_days_between_receipt --for interpretation: 0 --> just one receipt
,MIN(d.k_bon_dat) AS min_k_bon_dat
,MAX(d.k_bon_dat) AS max_k_bon_dat
FROM bdap_pilot_analysis.prj_cluster_base_bons_detail d   
LEFT JOIN bdap_pilot_analysis.prj_cluster_bons_with_date_shift s
ON  d.mandant_id = s.mandant_id
AND d.loyaltycardnumber	= s.loyaltycardnumber	
AND d.bon_id = s.bon_id		
GROUP BY 
 d.mandant_id
,d.loyaltycardnumber;


--checks
SELECT * 
FROM  bdap_pilot_analysis.prj_cluster_bons_with_card_and_date;

SELECT count(* )
FROM  bdap_pilot_analysis.prj_cluster_bons_with_card_and_date;



