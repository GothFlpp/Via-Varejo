SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [marketplace].[visao_sku]
(
	[idlojista] [int] NULL,
	[idsku] [int] NULL,
	[dtpedidovenda] [date] NULL,
	[bandeira] [nvarchar](256) NULL,
	[lojista] [nvarchar](256) NULL,
	[cluster] [nvarchar](256) NULL,
	[mundo] [nvarchar](256) NULL,
	[descricao_produto] [nvarchar](256) NULL,
	[descricao_sku] [nvarchar](256) NULL,
	[qtd_sku] [bigint] NOT NULL,
	[qtd_pedidos_colocados] [bigint] NOT NULL,
	[qtd_itens_colocados] [bigint] NOT NULL,
	[valorcolocado] [decimal](30, 4) NULL,
	[qtd_pedidos_aprovados] [bigint] NULL,
	[qtd_itens_aprovados] [bigint] NULL,
	[valoraprovado] [decimal](30, 4) NULL,
	[qtd_itens_enviados] [bigint] NULL,
	[valor_itens_enviados] [decimal](30, 4) NULL,
	[qtd_itens_entregues] [bigint] NULL,
	[valor_itens_entregues] [decimal](30, 4) NULL,
	[qtd_itens_cancelados] [bigint] NULL,
	[valor_itens_cancelados] [decimal](30, 4) NULL,
	[qtd_itens_devolvidos] [bigint] NULL,
	[valor_itens_devolvidos] [decimal](30, 4) NULL,
	[qtd_itens_atrasados] [bigint] NULL,
	[valor_itens_atrasados] [decimal](30, 4) NULL,
	[taxa_aprovacao] [decimal](38, 8) NULL,
	[data_atualizacao] [datetime2](7) NOT NULL,
	[year_partition] [nvarchar](256) NULL,
	[month_partition] [nvarchar](256) NULL,
	[day_partition] [nvarchar](256) NULL
)
WITH
(
	DISTRIBUTION = ROUND_ROBIN,
	CLUSTERED COLUMNSTORE INDEX
)
GO