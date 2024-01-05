SELECT 

quantidadegerada,
anogeracao,
split(lower(regexp_replace(detalhe, '[^a-zA-Z0-9áéíóúÁÉÍÓÚàèìòùÀÈÌÒÙäëïöüÄËÏÖÜâêîôûÂÊÎÔÛãñõÃÑÕ]', ' ')), ' ') as detalhe,
split(lower(regexp_replace(tiporesiduo, '[^a-zA-Z0-9áéíóúÁÉÍÓÚàèìòùÀÈÌÒÙäëïöüÄËÏÖÜâêîôûÂÊÎÔÛãñõÃÑÕ]', ' ')), ' ') as tiporesiduo,
split(lower(regexp_replace(categoriaatividade, '[^a-zA-Z0-9áéíóúÁÉÍÓÚàèìòùÀÈÌÒÙäëïöüÄËÏÖÜâêîôûÂÊÎÔÛãñõÃÑÕ]', ' ')), ' ') as categoriaatividade,
split(lower(regexp_replace(razaosocialgerador, '[^a-zA-Z0-9áéíóúÁÉÍÓÚàèìòùÀÈÌÒÙäëïöüÄËÏÖÜâêîôûÂÊÎÔÛãñõÃÑÕ]', ' ')), ' ') as razaosocialgerador

FROM myDataSource
WHERE unidade="Litro" AND classificacaoresiduo="Perigoso";