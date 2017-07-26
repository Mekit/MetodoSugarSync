-- SELECT * FROM IMP.dbo.ANAGRAFICACF WHERE CODCONTO = 'C  5171'


-- MESE IN CORSO
SELECT *
FROM IMP.dbo.Sog_SpecchiettoOrdiniClientiMeseInCorso
WHERE CodCliFor = 'C  3180';

-- ARTICOLO NON ACQUISTATI RECENTEMENTE
SELECT *
FROM IMP.dbo.Sog_SpecchiettoOrdiniClientiArticoliNonAcquistatiMesiRecenti
WHERE CodCliFor = 'C  1222';

-- SCADENZE
SELECT *
FROM IMP.dbo.Sog_SpecchiettoOrdiniClientiScadenzeAperte
WHERE CodCliForFatt = 'C  1222';

-- ARTICOLI ACQUISTATI
SELECT *
FROM IMP.dbo.Sog_SpecchiettoOrdiniClientiUltimiMesi
WHERE CodCliFor = 'C  1222'
ORDER BY TotRigaListino42 DESC;

