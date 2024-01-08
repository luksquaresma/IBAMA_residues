WITH 

splited AS (
    SELECT 

    quantidadegerada, --cast(regexp_replace(regexp_replace(quantidadegerada, '[.]', ''), ',', '.') as double) as quantidadegerada,
    anogeracao,
    CASE 
        WHEN unidade="Litro"      AND classificacaoresiduo="Perigoso"       THEN 'Líquido Perigoso'
        WHEN unidade="Litro"      AND classificacaoresiduo="Não Perigoso"   THEN 'Líquido Não Perigoso'
        WHEN unidade="kilogramas" AND classificacaoresiduo="Perigoso"       THEN 'Sólido Perigoso'
        WHEN unidade="kilogramas" AND classificacaoresiduo="Não Perigoso"   THEN 'Sólido Não Perigoso'
        WHEN unidade="Unidade" AND classificacaoresiduo="Perigoso"       THEN 'Outro Perigoso'
        WHEN unidade="Unidade" AND classificacaoresiduo="Não Perigoso"   THEN 'Outro Não Perigoso'
        ELSE 'Outro'
    END AS tipo,
    split(lower(regexp_replace(tiporesiduo, '[^a-zA-Z0-9áéíóúÁÉÍÓÚàèìòùÀÈÌÒÙäëïöüÄËÏÖÜâêîôûÂÊÎÔÛãñõÃÑÕçÇ°º]', ' ')), ' ') as tiporesiduo,
    split(lower(regexp_replace(detalhe, '[^a-zA-Z0-9áéíóúÁÉÍÓÚàèìòùÀÈÌÒÙäëïöüÄËÏÖÜâêîôûÂÊÎÔÛãñõÃÑÕçÇ°º]', ' ')), ' ') as detalhe,
    split(lower(regexp_replace(categoriaatividade, '[^a-zA-Z0-9áéíóúÁÉÍÓÚàèìòùÀÈÌÒÙäëïöüÄËÏÖÜâêîôûÂÊÎÔÛãñõÃÑÕçÇ°º]', ' ')), ' ') as categoriaatividade,
    split(lower(regexp_replace(razaosocialgerador, '[^a-zA-Z0-9áéíóúÁÉÍÓÚàèìòùÀÈÌÒÙäëïöüÄËÏÖÜâêîôûÂÊÎÔÛãñõÃÑÕçÇ°º]', ' ')), ' ') as razaosocialgerador

    FROM myDataSource
),

exp_detalhe AS (
    SELECT 
        word,
        anogeracao,
        tipo,
        SUM(COALESCE(CAST(detalhe AS FLOAT), 0)) AS detalhe
    FROM 
    (
        SELECT 
            explode(detalhe) AS word,
            anogeracao,
            tipo,
            CASE 
                WHEN quantidadegerada IS NULL OR quantidadegerada = '' THEN '0'
                ELSE quantidadegerada
            END AS detalhe
        FROM splited
    ) AS exploded_words
    WHERE NOT (word RLIKE '^[0-9]+([,.][0-9]+)?$') AND word != ''
    GROUP BY word, anogeracao, tipo
    ORDER BY word DESC
),

exp_tiporesiduo AS (
    SELECT 
        word,
        anogeracao,
        tipo,
        SUM(COALESCE(CAST(tiporesiduo AS FLOAT), 0)) AS tiporesiduo
    FROM 
    (
        SELECT 
            explode(tiporesiduo) AS word,
            anogeracao,
            tipo,
            CASE 
                WHEN quantidadegerada IS NULL OR quantidadegerada = '' THEN '0'
                ELSE quantidadegerada
            END AS tiporesiduo
        FROM splited
    ) AS exploded_words
    WHERE NOT (word RLIKE '^[0-9]+([,.][0-9]+)?$') AND word != ''
    GROUP BY word, anogeracao, tipo
    ORDER BY word DESC
),

exp_categoriaatividade AS (
    SELECT 
        word,
        anogeracao,
        tipo,
        SUM(COALESCE(CAST(categoriaatividade AS FLOAT), 0)) AS categoriaatividade
    FROM 
    (
        SELECT 
            explode(categoriaatividade) AS word,
            anogeracao,
            tipo,
            CASE 
                WHEN quantidadegerada IS NULL OR quantidadegerada = '' THEN '0'
                ELSE quantidadegerada
            END AS categoriaatividade
        FROM splited
    ) AS exploded_words
    WHERE NOT (word RLIKE '^[0-9]+([,.][0-9]+)?$') AND word != ''
    GROUP BY word, anogeracao, tipo
    ORDER BY word DESC
),

exp_razaosocialgerador AS (
    SELECT 
        word,
        anogeracao,
        tipo,
        SUM(COALESCE(CAST(razaosocialgerador AS FLOAT), 0)) AS razaosocialgerador
    FROM 
    (
        SELECT 
            explode(razaosocialgerador) AS word,
            anogeracao,
            tipo,
            CASE 
                WHEN quantidadegerada IS NULL OR quantidadegerada = '' THEN '0'
                ELSE quantidadegerada
            END AS razaosocialgerador
        FROM splited
    ) AS exploded_words
    WHERE NOT (word RLIKE '^[0-9]+([,.][0-9]+)?$') AND word != ''
    GROUP BY word, anogeracao, tipo
    ORDER BY word DESC
)

SELECT 
    COALESCE(
        exp_detalhe.word, 
        exp_tiporesiduo.word, 
        exp_categoriaatividade.word, 
        exp_razaosocialgerador.word
        ) AS word,
    COALESCE(
        exp_detalhe.anogeracao, 
        exp_tiporesiduo.anogeracao, 
        exp_categoriaatividade.anogeracao, 
        exp_razaosocialgerador.anogeracao
        ) AS anogeracao,
    COALESCE(
        exp_detalhe.tipo, 
        exp_tiporesiduo.tipo, 
        exp_categoriaatividade.tipo, 
        exp_razaosocialgerador.tipo
        ) AS tipo,  
    COALESCE(detalhe, 0) AS detalhe,
    COALESCE(tiporesiduo, 0) AS tiporesiduo,
    COALESCE(categoriaatividade, 0) AS categoriaatividade,
    COALESCE(razaosocialgerador, 0) AS razaosocialgerador

FROM exp_detalhe 
FULL OUTER JOIN exp_tiporesiduo 
ON exp_detalhe.word = exp_tiporesiduo.word 
AND exp_detalhe.anogeracao = exp_tiporesiduo.anogeracao
AND exp_detalhe.tipo = exp_tiporesiduo.tipo

FULL OUTER JOIN exp_categoriaatividade 
ON COALESCE(exp_detalhe.word, exp_tiporesiduo.word) = exp_categoriaatividade.word
AND COALESCE(exp_detalhe.anogeracao, exp_tiporesiduo.anogeracao) = exp_categoriaatividade.anogeracao
AND COALESCE(exp_detalhe.tipo, exp_tiporesiduo.tipo) = exp_categoriaatividade.tipo

FULL OUTER JOIN exp_razaosocialgerador 
ON COALESCE(exp_detalhe.word, exp_tiporesiduo.word, exp_categoriaatividade.word) = exp_razaosocialgerador.word
AND COALESCE(exp_detalhe.anogeracao, exp_tiporesiduo.anogeracao, exp_categoriaatividade.anogeracao) = exp_razaosocialgerador.anogeracao
AND COALESCE(exp_detalhe.tipo, exp_tiporesiduo.tipo, exp_categoriaatividade.tipo) = exp_razaosocialgerador.tipo

;