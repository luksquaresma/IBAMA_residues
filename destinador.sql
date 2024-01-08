WITH 

splited AS (
    SELECT 

    quantdestinada, -- cast(regexp_replace(regexp_replace(quantdestinada, '[.]', ''), ',', '.') as double) as quantdestinada,
    anoDestinacao,
    CASE 
        WHEN unidade="Litro (L)" THEN 'Líquido'
        WHEN unidade="kilogramas (kg)" THEN 'Sólido'
        WHEN unidade="Unidade (UN)" THEN 'Outro'
        ELSE 'Outro'
    END AS tipo,
    split(lower(regexp_replace(razaoSocialDestinador, '[^a-zA-Z0-9áéíóúÁÉÍÓÚàèìòùÀÈÌÒÙäëïöüÄËÏÖÜâêîôûÂÊÎÔÛãñõÃÑÕçÇ°º]', ' ')), ' ') as razaoSocialDestinador,
    split(lower(regexp_replace(detalhe, '[^a-zA-Z0-9áéíóúÁÉÍÓÚàèìòùÀÈÌÒÙäëïöüÄËÏÖÜâêîôûÂÊÎÔÛãñõÃÑÕçÇ°º]', ' ')), ' ') as detalhe,
    split(lower(regexp_replace(razaoSocialGeradorResiduo, '[^a-zA-Z0-9áéíóúÁÉÍÓÚàèìòùÀÈÌÒÙäëïöüÄËÏÖÜâêîôûÂÊÎÔÛãñõÃÑÕçÇ°º]', ' ')), ' ') as razaoSocialGeradorResiduo,
    split(lower(regexp_replace(descResiduo, '[^a-zA-Z0-9áéíóúÁÉÍÓÚàèìòùÀÈÌÒÙäëïöüÄËÏÖÜâêîôûÂÊÎÔÛãñõÃÑÕçÇ°º]', ' ')), ' ') as descResiduo

    FROM myDataSource
),

exp_razaoSocialDestinador AS (
    SELECT 
        word,
        anoDestinacao,
        tipo,
        SUM(COALESCE(CAST(razaoSocialDestinador AS FLOAT), 0)) AS razaoSocialDestinador
    FROM 
    (
        SELECT 
            explode(razaoSocialDestinador) AS word,
            anoDestinacao,
            tipo,
            CASE 
                WHEN quantdestinada IS NULL OR quantdestinada = '' THEN '0'
                ELSE quantdestinada
            END AS razaoSocialDestinador
        FROM splited
    ) AS exploded_words
    WHERE NOT (word RLIKE '^[0-9]+([,.][0-9]+)?$') AND word != ''
    GROUP BY word, anoDestinacao, tipo
    ORDER BY word DESC
),

exp_detalhe AS (
    SELECT 
        word,
        anoDestinacao,
        tipo,
        SUM(COALESCE(CAST(detalhe AS FLOAT), 0)) AS detalhe
    FROM 
    (
        SELECT 
            explode(detalhe) AS word,
            anoDestinacao,
            tipo,
            CASE 
                WHEN quantdestinada IS NULL OR quantdestinada = '' THEN '0'
                ELSE quantdestinada
            END AS detalhe
        FROM splited
    ) AS exploded_words
    WHERE NOT (word RLIKE '^[0-9]+([,.][0-9]+)?$') AND word != ''
    GROUP BY word, anoDestinacao, tipo
    ORDER BY word DESC
),

exp_razaoSocialGeradorResiduo AS (
    SELECT 
        word,
        anoDestinacao,
        tipo,
        SUM(COALESCE(CAST(razaoSocialGeradorResiduo AS FLOAT), 0)) AS razaoSocialGeradorResiduo
    FROM 
    (
        SELECT 
            explode(razaoSocialGeradorResiduo) AS word,
            anoDestinacao,
            tipo,
            CASE 
                WHEN quantdestinada IS NULL OR quantdestinada = '' THEN '0'
                ELSE quantdestinada
            END AS razaoSocialGeradorResiduo
        FROM splited
    ) AS exploded_words
    WHERE NOT (word RLIKE '^[0-9]+([,.][0-9]+)?$') AND word != ''
    GROUP BY word, anoDestinacao, tipo
    ORDER BY word DESC
),

exp_descResiduo AS (
    SELECT 
        word,
        anoDestinacao,
        tipo,
        SUM(COALESCE(CAST(descResiduo AS FLOAT), 0)) AS descResiduo
    FROM 
    (
        SELECT 
            explode(descResiduo) AS word,
            anoDestinacao,
            tipo,
            CASE 
                WHEN quantdestinada IS NULL OR quantdestinada = '' THEN '0'
                ELSE quantdestinada
            END AS descResiduo
        FROM splited
    ) AS exploded_words
    WHERE NOT (word RLIKE '^[0-9]+([,.][0-9]+)?$') AND word != ''
    GROUP BY word, anoDestinacao, tipo
    ORDER BY word DESC
)

SELECT 
    COALESCE(
        exp_razaoSocialDestinador.word, 
        exp_detalhe.word, 
        exp_razaoSocialGeradorResiduo.word, 
        exp_descResiduo.word
        ) AS word,
    COALESCE(
        exp_razaoSocialDestinador.anoDestinacao, 
        exp_detalhe.anoDestinacao, 
        exp_razaoSocialGeradorResiduo.anoDestinacao, 
        exp_descResiduo.anoDestinacao
        ) AS anoDestinacao,
    COALESCE(
        exp_razaoSocialDestinador.tipo, 
        exp_detalhe.tipo, 
        exp_razaoSocialGeradorResiduo.tipo, 
        exp_descResiduo.tipo
        ) AS tipo,  
    COALESCE(razaoSocialDestinador, 0) AS razaoSocialDestinador,
    COALESCE(detalhe, 0) AS detalhe,
    COALESCE(razaoSocialGeradorResiduo, 0) AS razaoSocialGeradorResiduo,
    COALESCE(descResiduo, 0) AS descResiduo

FROM exp_razaoSocialDestinador 
FULL OUTER JOIN exp_detalhe 
ON exp_razaoSocialDestinador.word = exp_detalhe.word 
AND exp_razaoSocialDestinador.anoDestinacao = exp_detalhe.anoDestinacao
AND exp_razaoSocialDestinador.tipo = exp_detalhe.tipo

FULL OUTER JOIN exp_razaoSocialGeradorResiduo 
ON COALESCE(exp_razaoSocialDestinador.word, exp_detalhe.word) = exp_razaoSocialGeradorResiduo.word
AND COALESCE(exp_razaoSocialDestinador.anoDestinacao, exp_detalhe.anoDestinacao) = exp_razaoSocialGeradorResiduo.anoDestinacao
AND COALESCE(exp_razaoSocialDestinador.tipo, exp_detalhe.tipo) = exp_razaoSocialGeradorResiduo.tipo

FULL OUTER JOIN exp_descResiduo 
ON COALESCE(exp_razaoSocialDestinador.word, exp_detalhe.word, exp_razaoSocialGeradorResiduo.word) = exp_descResiduo.word
AND COALESCE(exp_razaoSocialDestinador.anoDestinacao, exp_detalhe.anoDestinacao, exp_razaoSocialGeradorResiduo.anoDestinacao) = exp_descResiduo.anoDestinacao
AND COALESCE(exp_razaoSocialDestinador.tipo, exp_detalhe.tipo, exp_razaoSocialGeradorResiduo.tipo) = exp_descResiduo.tipo

;