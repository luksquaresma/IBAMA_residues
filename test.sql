SELECT 
COALESCE(c.word, d.word) AS word,
COALESCE(c.d_razaosocialgerador, 0) AS d_razaosocialgerador,
COALESCE(c.d_detalhe, 0) AS d_detalhe,
COALESCE(d.d_tiporesiduo, 0) AS d_tiporesiduo


FROM 
(
    SELECT 
    COALESCE(a.word, b.word) AS word,
    COALESCE(a.d_razaosocialgerador, 0) AS d_razaosocialgerador,
    COALESCE(b.d_detalhe, 0) AS d_detalhe
    
    FROM
    (
        (
            SELECT d_razaosocialgerador.d_razaosocialgerador AS word, 
            COUNT(*) AS d_razaosocialgerador
            FROM d_razaosocialgerador 
            GROUP BY d_razaosocialgerador
        ) AS a

        FULL OUTER JOIN

        (
            SELECT d_detalhe.d_detalhe AS word, 
            COUNT(*) AS d_detalhe
            FROM d_detalhe 
            GROUP BY d_detalhe
        ) AS b

        ON a.word = b.word
    )
) as c

FULL OUTER JOIN
(
    SELECT d_tiporesiduo.d_tiporesiduo AS word, 
    COUNT(*) AS d_tiporesiduo
    FROM d_tiporesiduo 
    GROUP BY d_tiporesiduo
) AS d

ON c.word = d.word;