SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [marketplace].[VisaoGeralProtocolos]
(
	[IdLojista] [int] NULL,
	[NmLojista] [nvarchar](256) NULL,
	[IdProtocolo] [bigint] NULL,
	[NrPedido] [nvarchar](256) NULL,
	[NrEntrega] [nvarchar](256) NULL,
	[NrSku] [int] NULL,
	[NmSku] [nvarchar](256) NULL,
	[VrUnitarioPedido] [float] NULL,
	[VrFreteUnitario] [float] NULL,
	[VrTotalPedido] [float] NULL,
	[DhCriacaoProtocolo] [datetime2](7) NULL,
	[DhAtualizacaoProtocolo] [datetime2](7) NULL,
	[DhFechamentoProtocolo] [datetime2](7) NULL,
	[QtHoraRespostaProtocolo] [float] NULL,
	[FlProtocoloSemResposta24Horas] [int] NOT NULL,
	[FlProtocoloSemResposta48Horas] [int] NOT NULL,
	[NmTipoEntregaCliente] [nvarchar](256) NULL,
	[NmCategoriaN1] [nvarchar](256) NULL,
	[NmCategoriaN2] [nvarchar](256) NULL,
	[NmCategoriaN3] [nvarchar](256) NULL,
	[NmProdutoN1] [nvarchar](256) NULL,
	[NmProdutoN2] [nvarchar](256) NULL,
	[NmProdutoN3] [nvarchar](256) NULL,
	[StProtocolo] [nvarchar](256) NOT NULL,
	[FlProtocoloReclamacao] [int] NOT NULL,
	[FlCanalEspecial] [int] NOT NULL,
	[NmTipoProtocoloInicial] [nvarchar](256) NULL,
	[NmTipoProtocoloAtual] [nvarchar](256) NULL,
	[NmFilaAtual] [nvarchar](256) NULL,
	[NmFilaAnterior] [nvarchar](256) NULL,
	[FlProtocoloMarketPlace] [int] NOT NULL,
	[FlProtocoloLojista] [int] NOT NULL,
	[QtRecontato] [int] NULL,
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



SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [marketplace].[VisaoLojistaProtocolos]
(
	[idlojista] [int] NULL,
	[DhCriacaoProtocolo] [date] NULL,
	[QtProtocolos] [bigint] NOT NULL,
	[VrTotalProtocolos] [float] NULL,
	[VrTotalProtocolosAbs] [float] NULL,
	[QtProtocolosAbertos] [bigint] NOT NULL,
	[QtProtocolosSemResposta24Horas] [bigint] NOT NULL,
	[QtProtocolosSemResposta48Horas] [bigint] NOT NULL,
	[QtProtocolosReclamacao] [bigint] NOT NULL,
	[QtProtocolosAbertosReclamacao] [bigint] NOT NULL,
	[VrTotalProtocoloReclamacao] [float] NULL,
	[VrTotalProtocoloReclamacaoAbs] [float] NULL,
	[QtTotalRecontatos] [bigint] NULL,
	[QtTotalCanaisEspeciais] [bigint] NOT NULL,
	[QtTotalFilaMarketPlace] [bigint] NOT NULL,
	[QtTotalFilaLojista] [bigint] NOT NULL,
	[TxReclamacao] [float] NULL,
	[DtAtualizacao] [datetime2](7) NOT NULL,
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
