-- 06 · Views "ciclo atual" da Gold no SQL Server
-- Squad 2 / Dupla 1
--
-- ALTERNATIVA OPCIONAL, não necessária para o BI funcionar. O notebook 05 já regrava (overwrite+truncate)
-- tabelas "<tabela>_atual" a cada ciclo, via format("sqlserver") nativo — é o que o BI usa por padrão
-- (ver "Tabelas atual no SQL Server" no 05). Uma tentativa anterior de criar estas views via pymssql
-- travou o kernel do notebook com SIGABRT (incompatibilidade binária com o runtime Serverless) e foi
-- revertida — ver nota no 05 (08/10). Rode este script manualmente só se preferir views de verdade em vez
-- das tabelas "_atual" (SSMS / Azure Data Studio, usuário com CREATE VIEW no schema dbo).
-- É idempotente: pode ser reexecutado quando uma tabela Gold ganhar coluna nova (view com SELECT *
-- congela a lista de colunas no momento da criação).
--
-- Por que existe: as tabelas gold_kpi_* acumulam um ciclo por execução (append). Somar sem filtrar
-- conta o mesmo SKU/categoria uma vez por ciclo. O Looker deve usar:
--   * vw_gold_kpi_*_atual  -> estado atual do catálogo (placar, rankings, diagnósticos)
--   * gold_kpi_* / gold_hist_*  -> evolução no tempo (sempre com ciclo_id no eixo ou no filtro)
--
-- Tabelas devem existir (rodar o notebook 05 ao menos uma vez antes).

-- Índice em ciclo_id: o MAX(ciclo_id) e o filtro das views viram busca por índice em vez de varredura.
-- Guardado por IF: não falha se a tabela ainda não existir nem se o índice já existir.
DECLARE @tabelas TABLE (nome SYSNAME);
INSERT INTO @tabelas VALUES
    ('gold_kpi_catalogo_resumo'), ('gold_kpi_categoria_performance'), ('gold_kpi_categoria_raiz'),
    ('gold_kpi_faixa_preco'), ('gold_kpi_qualidade_dados'), ('gold_kpi_rejeitados_produtos');

DECLARE @nome SYSNAME, @sql NVARCHAR(MAX);
DECLARE c CURSOR LOCAL FAST_FORWARD FOR SELECT nome FROM @tabelas;
OPEN c;
FETCH NEXT FROM c INTO @nome;
WHILE @@FETCH_STATUS = 0
BEGIN
    IF OBJECT_ID(N'dbo.' + @nome, N'U') IS NOT NULL
       AND NOT EXISTS (SELECT 1 FROM sys.indexes WHERE object_id = OBJECT_ID(N'dbo.' + @nome) AND name = N'ix_' + @nome + N'_ciclo_id')
    BEGIN
        SET @sql = N'CREATE NONCLUSTERED INDEX ix_' + @nome + N'_ciclo_id ON dbo.' + QUOTENAME(@nome) + N' (ciclo_id);';
        EXEC sp_executesql @sql;
    END
    FETCH NEXT FROM c INTO @nome;
END
CLOSE c;
DEALLOCATE c;
GO

CREATE OR ALTER VIEW dbo.vw_gold_kpi_catalogo_resumo_atual AS
SELECT t.* FROM dbo.gold_kpi_catalogo_resumo AS t
WHERE t.ciclo_id = (SELECT MAX(ciclo_id) FROM dbo.gold_kpi_catalogo_resumo);
GO

CREATE OR ALTER VIEW dbo.vw_gold_kpi_categoria_performance_atual AS
SELECT t.* FROM dbo.gold_kpi_categoria_performance AS t
WHERE t.ciclo_id = (SELECT MAX(ciclo_id) FROM dbo.gold_kpi_categoria_performance);
GO

CREATE OR ALTER VIEW dbo.vw_gold_kpi_categoria_raiz_atual AS
SELECT t.* FROM dbo.gold_kpi_categoria_raiz AS t
WHERE t.ciclo_id = (SELECT MAX(ciclo_id) FROM dbo.gold_kpi_categoria_raiz);
GO

CREATE OR ALTER VIEW dbo.vw_gold_kpi_faixa_preco_atual AS
SELECT t.* FROM dbo.gold_kpi_faixa_preco AS t
WHERE t.ciclo_id = (SELECT MAX(ciclo_id) FROM dbo.gold_kpi_faixa_preco);
GO

CREATE OR ALTER VIEW dbo.vw_gold_kpi_qualidade_dados_atual AS
SELECT t.* FROM dbo.gold_kpi_qualidade_dados AS t
WHERE t.ciclo_id = (SELECT MAX(ciclo_id) FROM dbo.gold_kpi_qualidade_dados);
GO

CREATE OR ALTER VIEW dbo.vw_gold_kpi_rejeitados_produtos_atual AS
SELECT t.* FROM dbo.gold_kpi_rejeitados_produtos AS t
WHERE t.ciclo_id = (SELECT MAX(ciclo_id) FROM dbo.gold_kpi_rejeitados_produtos);
GO
