#visao geral


SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [marketplace].[visao_geral]
(
	[DtPedidoVenda] [datetime2](7) NULL,
	[dataaprovacao] [datetime2](7) NULL,
	[datacriacaoregistro] [datetime2](7) NULL,
	[bandeira] [nvarchar](256) NULL,
	[flagaprovado] [int] NOT NULL,
	[idcanalvenda] [nvarchar](256) NULL,
	[idcompra] [int] NULL,
	[idcliente] [int] NULL,
	[idlistadecompra] [int] NULL,
	[NmUnidadeNegocio] [nvarchar](256) NULL,
	[NmTipoEntrega] [nvarchar](256) NULL,
	[idcompraentregastatus] [nvarchar](256) NULL,
	[NmSituacaoEntregaSite] [nvarchar](256) NULL,
	[dataprevisao] [datetime2](7) NULL,
	[datastatus] [datetime2](7) NULL,
	[dataentrega] [datetime2](7) NULL,
	[gerencialid] [bigint] NULL,
	[dataemissaonotafiscal] [datetime2](7) NULL,
	[dataentregacorrigida] [datetime2](7) NULL,
	[dataprometidaoriginal] [datetime2](7) NULL,
	[chave_nfe] [nvarchar](256) NULL,
	[idlojista] [int] NULL,
	[lojista] [nvarchar](256) NULL,
	[status_lojista] [bit] NULL,
	[datalimitesaidacd] [datetime2](7) NULL,
	[dataentregaajustada] [datetime2](7) NULL,
	[idsku] [int] NULL,
	[descricao_sku] [nvarchar](256) NULL,
	[descricao_produto] [nvarchar](256) NULL,
	[valorvendaunidade] [decimal](19, 4) NULL,
	[valorvendaunidademenoscupomnominal] [decimal](19, 4) NULL,
	[valorfretecomdesconto] [decimal](19, 4) NULL,
	[cluster] [nvarchar](256) NULL,
	[mundo] [nvarchar](256) NULL,
	[idtransportadora] [bigint] NULL,
	[nometransportadora] [nvarchar](256) NULL,
	[today] [date] NOT NULL,
	[row_number_itens] [int] NULL,
	[flagidcompraunico] [int] NULL,
	[flagaprovadounico] [int] NOT NULL,
	[valortotal] [decimal](20, 4) NULL,
	[status_pedido] [nvarchar](256) NOT NULL,
	[prazo_total] [int] NULL,
	[prazo_restante] [int] NULL,
	[risco_atraso] [float] NULL,
	[base_gap] [nvarchar](256) NULL,
	[data_atualizacao] [datetime2](7) NOT NULL,
	[year_partition] [nvarchar](256) NULL,
	[month_partition] [nvarchar](256) NULL,
	[day_partition] [nvarchar](256) NULL
)
WITH
(
	DISTRIBUTION = HASH ( [idlojista] ),
	CLUSTERED COLUMNSTORE INDEX
)
GO





#visao lojista

SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [marketplace].[visao_lojista]
(
	[idlojista] [int] NULL,
	[dtpedidovenda] [date] NULL,
	[dataprevisao] [date] NULL,
	[bandeira] [nvarchar](256) NULL,
	[lojista] [nvarchar](256) NULL,
	[cluster] [nvarchar](256) NULL,
	[mundo] [nvarchar](256) NULL,
	[status_lojista] [nvarchar](256) NULL,
	[qtd_pedidos_colocados] [bigint] NOT NULL,
	[qtd_itens_colocados] [bigint] NOT NULL,
	[qtd_pedidos_aprovados] [bigint] NULL,
	[qtd_itens_aprovados] [bigint] NULL,
	[valorcolocado] [decimal](30, 4) NULL,
	[valoraprovado] [decimal](30, 4) NULL,
	[qtd_pedidos_atrasados_envio] [bigint] NULL,
	[valor_pedidos_atrasados_envio] [decimal](30, 4) NULL,
	[qtd_pedidos_atraso_entrega] [bigint] NULL,
	[valor_pedidos_atraso_entrega] [decimal](30, 4) NULL,
	[qtd_itens_no_prazo] [bigint] NULL,
	[valor_itens_no_prazo] [decimal](30, 4) NULL,
	[qtd_itens_risco_atraso] [bigint] NULL,
	[valor_itens_risco_atraso] [decimal](30, 4) NULL,
	[qtd_itens_altorisco_atraso] [bigint] NULL,
	[valor_itens_altorisco_atraso] [decimal](30, 4) NULL,
	[qtd_itens_enviados] [bigint] NULL,
	[valor_itens_enviados] [decimal](30, 4) NULL,
	[qtd_itens_entregues] [bigint] NULL,
	[valor_itens_entregues] [decimal](30, 4) NULL,
	[qtd_itens_cancelados] [bigint] NULL,
	[valor_itens_cancelados] [decimal](30, 4) NULL,
	[qtd_itens_devolvidos] [bigint] NULL,
	[valor_itens_devolvidos] [decimal](30, 4) NULL,
	[qtd_sku_distinto] [bigint] NOT NULL,
	[qtd_sku] [bigint] NOT NULL,
	[taxa_aprovacao] [decimal](38, 8) NULL,
	[taxa_atraso_envio] [decimal](38, 8) NULL,
	[taxa_atraso_entrega] [decimal](38, 8) NULL,
	[taxa_risco_atraso_entrega] [decimal](38, 8) NULL,
	[taxa_altorisco_atraso_entrega] [decimal](38, 8) NULL,
	[data_atualizacao] [datetime2](7) NOT NULL,
	[year_partition] [nvarchar](256) NULL,
	[month_partition] [nvarchar](256) NULL,
	[day_partition] [nvarchar](256) NULL
)
WITH
(
	DISTRIBUTION = HASH ( [idlojista] ),
	CLUSTERED COLUMNSTORE INDEX
)
GO




#visao sku


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
	[qtd_itens_aprovados] [bigint] NULL,
	[qtd_pedidos_aprovados] [bigint] NULL,
	[valorcolocado] [decimal](30, 4) NULL,
	[valoraprovado] [decimal](30, 4) NULL,
	[taxa_aprovacao] [decimal](38, 8) NULL,
	[data_atualizacao] [datetime2](7) NOT NULL,
	[year_partition] [nvarchar](256) NULL,
	[month_partition] [nvarchar](256) NULL,
	[day_partition] [nvarchar](256) NULL
)
WITH
(
	DISTRIBUTION = HASH ( [idlojista] ),
	CLUSTERED COLUMNSTORE INDEX
)
GO
