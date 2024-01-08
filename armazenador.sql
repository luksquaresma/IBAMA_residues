WITH 

splited AS (
    SELECT 

    qtdarmazenviadadestfinal, -- cast(regexp_replace(regexp_replace(qtdarmazenviadadestfinal, '[.]', ''), ',', '.') as double) as qtdarmazenviadadestfinal,
    anoDestinacao,
    CASE 
        WHEN unidade="Litro (L)" THEN 'Líquido'
        WHEN unidade="kilogramas (kg)" THEN 'Sólido'
        WHEN unidade="Unidade (UN)" THEN 'Outro'
        ELSE 'Outro'
    END AS tipo,
    split(lower(regexp_replace(razsocempresaarmazdest, '[^a-zA-Z0-9áéíóúÁÉÍÓÚàèìòùÀÈÌÒÙäëïöüÄËÏÖÜâêîôûÂÊÎÔÛãñõÃÑÕçÇ°º]', ' ')), ' ') as razsocempresaarmazdest,
    split(lower(regexp_replace(razaosocialempgeradoraresiduo, '[^a-zA-Z0-9áéíóúÁÉÍÓÚàèìòùÀÈÌÒÙäëïöüÄËÏÖÜâêîôûÂÊÎÔÛãñõÃÑÕçÇ°º]', ' ')), ' ') as razaosocialempgeradoraresiduo,
    split(lower(regexp_replace(razaosocialarmazenador, '[^a-zA-Z0-9áéíóúÁÉÍÓÚàèìòùÀÈÌÒÙäëïöüÄËÏÖÜâêîôûÂÊÎÔÛãñõÃÑÕçÇ°º]', ' ')), ' ') as razaosocialarmazenador,
    split(lower(regexp_replace(descresiduo, '[^a-zA-Z0-9áéíóúÁÉÍÓÚàèìòùÀÈÌÒÙäëïöüÄËÏÖÜâêîôûÂÊÎÔÛãñõÃÑÕçÇ°º]', ' ')), ' ') as descresiduo

    FROM myDataSource
),

exp_razsocempresaarmazdest AS (
    SELECT 
        word,
        anoDestinacao,
        tipo,
        SUM(COALESCE(CAST(razsocempresaarmazdest AS FLOAT), 0)) AS razsocempresaarmazdest
    FROM 
    (
        SELECT 
            explode(razsocempresaarmazdest) AS word,
            anoDestinacao,
            tipo,
            CASE 
                WHEN qtdarmazenviadadestfinal IS NULL OR qtdarmazenviadadestfinal = '' THEN '0'
                ELSE qtdarmazenviadadestfinal
            END AS razsocempresaarmazdest
        FROM splited
    ) AS exploded_words
    WHERE NOT (word RLIKE '^[0-9]+([,.][0-9]+)?$') AND word != ''
    GROUP BY word, anoDestinacao, tipo
    ORDER BY word DESC
),

exp_razaosocialempgeradoraresiduo AS (
    SELECT 
        word,
        anoDestinacao,
        tipo,
        SUM(COALESCE(CAST(razaosocialempgeradoraresiduo AS FLOAT), 0)) AS razaosocialempgeradoraresiduo
    FROM 
    (
        SELECT 
            explode(razaosocialempgeradoraresiduo) AS word,
            anoDestinacao,
            tipo,
            CASE 
                WHEN qtdarmazenviadadestfinal IS NULL OR qtdarmazenviadadestfinal = '' THEN '0'
                ELSE qtdarmazenviadadestfinal
            END AS razaosocialempgeradoraresiduo
        FROM splited
    ) AS exploded_words
    WHERE NOT (word RLIKE '^[0-9]+([,.][0-9]+)?$') AND word != ''
    GROUP BY word, anoDestinacao, tipo
    ORDER BY word DESC
),

exp_razaosocialarmazenador AS (
    SELECT 
        word,
        anoDestinacao,
        tipo,
        SUM(COALESCE(CAST(razaosocialarmazenador AS FLOAT), 0)) AS razaosocialarmazenador
    FROM 
    (
        SELECT 
            explode(razaosocialarmazenador) AS word,
            anoDestinacao,
            tipo,
            CASE 
                WHEN qtdarmazenviadadestfinal IS NULL OR qtdarmazenviadadestfinal = '' THEN '0'
                ELSE qtdarmazenviadadestfinal
            END AS razaosocialarmazenador
        FROM splited
    ) AS exploded_words
    WHERE NOT (word RLIKE '^[0-9]+([,.][0-9]+)?$') AND word != ''
    GROUP BY word, anoDestinacao, tipo
    ORDER BY word DESC
),

exp_descresiduo AS (
    SELECT 
        word,
        anoDestinacao,
        tipo,
        SUM(COALESCE(CAST(descresiduo AS FLOAT), 0)) AS descresiduo
    FROM 
    (
        SELECT 
            explode(descresiduo) AS word,
            anoDestinacao,
            tipo,
            CASE 
                WHEN qtdarmazenviadadestfinal IS NULL OR qtdarmazenviadadestfinal = '' THEN '0'
                ELSE qtdarmazenviadadestfinal
            END AS descresiduo
        FROM splited
    ) AS exploded_words
    WHERE NOT (word RLIKE '^[0-9]+([,.][0-9]+)?$') AND word != ''
    GROUP BY word, anoDestinacao, tipo
    ORDER BY word DESC
)

SELECT 
    COALESCE(
        exp_razsocempresaarmazdest.word, 
        exp_razaosocialempgeradoraresiduo.word, 
        exp_razaosocialarmazenador.word, 
        exp_descresiduo.word
        ) AS word,
    COALESCE(
        exp_razsocempresaarmazdest.anoDestinacao, 
        exp_razaosocialempgeradoraresiduo.anoDestinacao, 
        exp_razaosocialarmazenador.anoDestinacao, 
        exp_descresiduo.anoDestinacao
        ) AS anoDestinacao,
    COALESCE(
        exp_razsocempresaarmazdest.tipo, 
        exp_razaosocialempgeradoraresiduo.tipo, 
        exp_razaosocialarmazenador.tipo, 
        exp_descresiduo.tipo
        ) AS tipo,  
    COALESCE(razsocempresaarmazdest, 0) AS razsocempresaarmazdest,
    COALESCE(razaosocialempgeradoraresiduo, 0) AS razaosocialempgeradoraresiduo,
    COALESCE(razaosocialarmazenador, 0) AS razaosocialarmazenador,
    COALESCE(descresiduo, 0) AS descresiduo

FROM exp_razsocempresaarmazdest 
FULL OUTER JOIN exp_razaosocialempgeradoraresiduo 
ON exp_razsocempresaarmazdest.word = exp_razaosocialempgeradoraresiduo.word 
AND exp_razsocempresaarmazdest.anoDestinacao = exp_razaosocialempgeradoraresiduo.anoDestinacao
AND exp_razsocempresaarmazdest.tipo = exp_razaosocialempgeradoraresiduo.tipo

FULL OUTER JOIN exp_razaosocialarmazenador 
ON COALESCE(exp_razsocempresaarmazdest.word, exp_razaosocialempgeradoraresiduo.word) = exp_razaosocialarmazenador.word
AND COALESCE(exp_razsocempresaarmazdest.anoDestinacao, exp_razaosocialempgeradoraresiduo.anoDestinacao) = exp_razaosocialarmazenador.anoDestinacao
AND COALESCE(exp_razsocempresaarmazdest.tipo, exp_razaosocialempgeradoraresiduo.tipo) = exp_razaosocialarmazenador.tipo

FULL OUTER JOIN exp_descresiduo 
ON COALESCE(exp_razsocempresaarmazdest.word, exp_razaosocialempgeradoraresiduo.word, exp_razaosocialarmazenador.word) = exp_descresiduo.word
AND COALESCE(exp_razsocempresaarmazdest.anoDestinacao, exp_razaosocialempgeradoraresiduo.anoDestinacao, exp_razaosocialarmazenador.anoDestinacao) = exp_descresiduo.anoDestinacao
AND COALESCE(exp_razsocempresaarmazdest.tipo, exp_razaosocialempgeradoraresiduo.tipo, exp_razaosocialarmazenador.tipo) = exp_descresiduo.tipo

;