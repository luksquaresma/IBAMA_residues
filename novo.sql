detalhe
tiporesiduo
categoriaatividade
razaosocialgerador


SELECT
json_array_elements_text(detalhe::json) AS element
FROM myDataSource 




SELECT 
    COALESCE(a.word, b.word) AS word,
    COALESCE(a.detalhe, 0) AS detalhe,
    COALESCE(b.tiporesiduo, 0) AS tiporesiduo
FROM 
    (
        SELECT detalhe AS word, 
        COUNT(*) AS detalhe
        FROM myDataSource 
        GROUP BY detalhe
    ) AS a
    
    FULL OUTER JOIN 

    (
        SELECT tiporesiduo AS word, 
        COUNT(*) AS tiporesiduo
        FROM myDataSource 
        GROUP BY tiporesiduo
    ) AS b

ON a.word = b.word;



SELECT

COALESCE(c1.word, c2.word) AS word,
COALESCE(c1.detalhe, 0) AS detalhe,
COALESCE(c1.tiporesiduo, 0) AS tiporesiduo,
COALESCE(c2.categoriaatividade, 0) AS categoriaatividade,
COALESCE(c2.razaosocialgerador, 0) AS razaosocialgerador

FROM 
(
    SELECT 
    COALESCE(a1.word, b1.word) AS word,
    COALESCE(a1.detalhe, 0) AS detalhe,
    COALESCE(b1.tiporesiduo, 0) AS tiporesiduo
    
    FROM
    (
        (
            
            SELECT detalhe AS word, 
            COUNT(*) AS detalhe
            FROM myDataSource 
            GROUP BY detalhe
        ) AS a1

        FULL OUTER JOIN

        (
            SELECT tiporesiduo AS word, 
            COUNT(*) AS tiporesiduo
            FROM myDataSource 
            GROUP BY tiporesiduo
        ) AS b1

        ON a1.word = b1.word
    )
) as c1

FULL OUTER JOIN

(
    SELECT 
    COALESCE(a2.word, b2.word) AS word,
    COALESCE(a2.categoriaatividade, 0) AS categoriaatividade,
    COALESCE(b2.razaosocialgerador, 0) AS razaosocialgerador
    
    FROM
    (
        (
            SELECT categoriaatividade AS word, 
            COUNT(*) AS categoriaatividade
            FROM myDataSource 
            GROUP BY categoriaatividade
        ) AS a2

        FULL OUTER JOIN

        (
            SELECT razaosocialgerador AS word, 
            COUNT(*) AS razaosocialgerador
            FROM myDataSource 
            GROUP BY razaosocialgerador
        ) AS b2

        ON a2.word = b2.word
    )
) as c2

ON c1.word = c2.word;