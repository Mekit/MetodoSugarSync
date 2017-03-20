SELECT TOP 1000
  ACF.CODCONTO      AS CodiceMetodo,
  ACFR.CODCONTOFATT AS ClienteDiFatturazione,
  ACF.CODFISCALE    AS CodiceFiscale,
  ACF.PARTITAIVA    AS PartitaIva,
  ACF.DSCCONTO1     AS Nome1,

  (CASE WHEN EXTC.SOGCRM_Esportabile IS NOT NULL
    THEN EXTC.SOGCRM_Esportabile
   WHEN EXTF.SOGCRM_Esportabile IS NOT NULL
     THEN EXTF.SOGCRM_Esportabile
   ELSE 1 END)      AS CrmExportFlag
FROM IMP.[dbo].[ANAGRAFICACF] AS ACF
  INNER JOIN IMP.[dbo].[ANAGRAFICARISERVATICF] AS ACFR
    ON ACF.CODCONTO = ACFR.CODCONTO AND ACFR.ESERCIZIO = (SELECT TOP (1) TE.CODICE
                                                          FROM IMP.[dbo].[TABESERCIZI] AS TE
                                                          ORDER BY TE.CODICE DESC)
  LEFT JOIN IMP.dbo.EXTRACLIENTI AS EXTC ON ACF.CODCONTO = EXTC.CODCONTO
  LEFT JOIN IMP.dbo.EXTRAFORNITORI AS EXTF ON ACF.CODCONTO = EXTF.CODCONTO
WHERE (CASE WHEN EXTC.SOGCRM_Esportabile IS NOT NULL
  THEN EXTC.SOGCRM_Esportabile
       WHEN EXTF.SOGCRM_Esportabile IS NOT NULL
         THEN EXTF.SOGCRM_Esportabile
       ELSE 1 END) <> 1
ORDER BY ACF.CODCONTO ASC
