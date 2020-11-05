--1)Assumidos pelo marketplace + texto de resposta do lojista:
--PS1. Para obter o texto da resposta e toda a conversa basta acessar a tabela ac_admin.ATD_TICKET_LOG
--PS2. Esta query demora porque não temos registros nesta situação
--a) sem resposta lojista
select distinct /*PARALLEL(20)*/ * from (
    select distinct
        t.dat_created, --data da abertura do protocolo
        t.dat_closed, --data do fechamento do protocolo caso tenha sido fechado
        sysdate - t.dat_created tempo_aberto, --tempo que ele está aberto (considerar apenas se a data fechamento não foi preenchida)
        t.expiration_date,  --data máxima para atendimento
        t.cod_ticket, --identificação do protocolo
        t.id_order, --id do pedido no Marketplace (difere um pouquinho do pedido do FRONT)
        (select nam_ticket_subject from ac_admin.atd_ticket_subject where id_ticket_subject = t.id_ticket_subject) tipo, --está na tabela de domínio ac_admin.atd_ticket_subject
        (select nam_reason from ac_admin.atd_reason where id_reason = t.id_reason) motivo, --está na tabela de domínio ac_admin.atd_reason
        t.txt_email, --email do cliente do pedido
        case
            when t.cod_responsible = 0 then 'Marketplace'
            else (select store_name from ac_admin.ECAD_SELLER_STORE_INFO_VW where id = t.cod_responsible)
        end responsavel, --indica se o responsável atual é o Marketplace ou o lojista
        case
            when id_status = 1 then 'Aberto'
            when id_status = 2 then 'Fechado'
            when id_status = 6 then 'Sige'
            else 'N/A'
        end status, --indica os status atual do protocolo
        (select id_user from ac_admin.ATD_TICKET_LOG where id_ticket = t.id_ticket and rownum <= 1) usuario_resposta_lojista, --caso o lojista tenha dado alguma resposta, este campo vai mostrar o usuário que fez a primeira resposta; para pegar todas as respostas basta acesar os registros da tabela ac_admin.ATD_TICKET_LOG
        t.store_name, --nome do lojista do pedido
        (
            select nam_refund_reason from 
                ac_admin.ATD_REFUND_REASON rr 
            inner join
                ac_admin.ATD_ORDER_INFO oi
            on rr.id_refund_reason = oi.id_refund_reason
            where oi.id_ticket = t.id_ticket
            and rownum <= 1
        ) motivo_cancelamento_devolucao, --está na tabela de domínio ac_admin.ATD_REFUND_REASON (pode ter mais de uma linha porque quando lojista autoriza o cancelamento/devolução é adicionado mais um registro à esta tabela)
        case (
            select max(flg_approved) from 
                ac_admin.ATD_ORDER_INFO oi
            where oi.id_ticket = t.id_ticket
            group by id_ticket
        ) when 0 then 'Não'
          when 1 then 'Sim'
          else null
        end cancelamento_devolucao_aprovada, --indica se o cancelamento/devolução foi aprovado ou não; quando está nulo é porque o protocolo é de um outro tipo
        (select type_opener from ac_admin.ATD_TYPE_OPENER where id_type_opener = t.id_type_opener) origem_abertura,  --está na tabela de domínio ac_admin.ATD_TYPE_OPENER 
        (
        select S.nam_source 
        from 
            ac_admin.ATD_TICKET_COMPLAINT C
        inner join
            ac_admin.ATD_TICKET_COMPLAINT_SOURCE S
        on
            c.id_complaint_source = s.id_ticket_complaint_source
        where c.id_ticket = t.id_ticket and rownum <= 1
        ) origem_reclamacao_critica, ----está na tabela de domínio ac_admin.ATD_TICKET_COMPLAINT_SOURCE e indica caso tenha sido exposto a reclamação em redes sociais, Reclame aqui, etc.
        flg_complaint em_ouvidoria -- 1 indica que sim, o protocolo está em ouvidoria e 0 indica que não está
    from 
        ac_admin.ATD_TICKET T  --tabela principal de tickets
    where id_store = 'NCMP'  --apenas registros do ambiente do Marketplace
        and ID_STATUS = 1  --apenas protocolos aberto
        and COD_OWNER != 0  --pega apenas os protocolos que o dono não é o Marketplace
        and COD_OWNER not in (select seller_id from ac_admin.NPC_CONF_VV) --pega apenas os protocolos que os lojistas não são Retira
        and COD_RESPONSIBLE = 0 --pega apenas os registros que o responsável é o Marketplace
) where usuario_resposta_lojista is null;



--a) com resposta lojista
select distinct /*PARALLEL(20)*/ * from (
    select
        t.dat_created, --data da abertura do protocolo
        t.dat_closed, --data do fechamento do protocolo caso tenha sido fechado
        sysdate - t.dat_created tempo_aberto, --tempo que ele está aberto (considerar apenas se a data fechamento não foi preenchida)
        t.expiration_date,  --data máxima para atendimento
        t.cod_ticket, --identificação do protocolo
        t.id_order, --id do pedido no Marketplace (difere um pouquinho do pedido do FRONT)
        (select nam_ticket_subject from ac_admin.atd_ticket_subject where id_ticket_subject = t.id_ticket_subject) tipo, --está na tabela de domínio ac_admin.atd_ticket_subject
        (select nam_reason from ac_admin.atd_reason where id_reason = t.id_reason) motivo, --está na tabela de domínio ac_admin.atd_reason
        t.txt_email, --email do cliente do pedido
        case
            when t.cod_responsible = 0 then 'Marketplace'
            else (select store_name from ac_admin.ECAD_SELLER_STORE_INFO_VW where id = t.cod_responsible)
        end responsavel, --indica se o responsável atual é o Marketplace ou o lojista
        case
            when id_status = 1 then 'Aberto'
            when id_status = 2 then 'Fechado'
            when id_status = 6 then 'Sige'
            else 'N/A'
        end status, --indica os status atual do protocolo
        (select id_user from ac_admin.ATD_TICKET_LOG where id_ticket = t.id_ticket and rownum <= 1) usuario_resposta_lojista, --caso o lojista tenha dado alguma resposta, este campo vai mostrar o usuário que fez a primeira resposta; para pegar todas as respostas basta acesar os registros da tabela ac_admin.ATD_TICKET_LOG
        t.store_name, --nome do lojista do pedido
        (
            select nam_refund_reason from 
                ac_admin.ATD_REFUND_REASON rr 
            inner join
                ac_admin.ATD_ORDER_INFO oi
            on rr.id_refund_reason = oi.id_refund_reason
            where oi.id_ticket = t.id_ticket
            and rownum <= 1
        ) motivo_cancelamento_devolucao, --está na tabela de domínio ac_admin.ATD_REFUND_REASON (pode ter mais de uma linha porque quando lojista autoriza o cancelamento/devolução é adicionado mais um registro à esta tabela)
        case (
            select max(flg_approved) from 
                ac_admin.ATD_ORDER_INFO oi
            where oi.id_ticket = t.id_ticket
            group by id_ticket
        ) when 0 then 'Não'
          when 1 then 'Sim'
          else null
        end cancelamento_devolucao_aprovada, --indica se o cancelamento/devolução foi aprovado ou não; quando está nulo é porque o protocolo é de um outro tipo
        (select type_opener from ac_admin.ATD_TYPE_OPENER where id_type_opener = t.id_type_opener) origem_abertura,  --está na tabela de domínio ac_admin.ATD_TYPE_OPENER 
        (
        select S.nam_source 
        from 
            ac_admin.ATD_TICKET_COMPLAINT C
        inner join
            ac_admin.ATD_TICKET_COMPLAINT_SOURCE S
        on
            c.id_complaint_source = s.id_ticket_complaint_source
        where c.id_ticket = t.id_ticket and rownum <= 1
        ) origem_reclamacao_critica, ----está na tabela de domínio ac_admin.ATD_TICKET_COMPLAINT_SOURCE e indica caso tenha sido exposto a reclamação em redes sociais, Reclame aqui, etc.
        FLG_COMPLAINT em_ouvidoria -- 1 indica que sim, o protocolo está em ouvidoria e 0 indica que não está
    from 
        ac_admin.ATD_TICKET T  --tabela principal de tickets
    where id_store = 'NCMP'  --apenas registros do ambiente do Marketplace
        and ID_STATUS = 1  --apenas protocolos aberto
        and COD_OWNER != 0  --pega apenas os protocolos que o dono não é o Marketplace
        and COD_OWNER not in (select seller_id from ac_admin.NPC_CONF_VV) --pega apenas os protocolos que os lojistas não são Retira
        and COD_RESPONSIBLE = 0 --pega apenas os registros que o responsável é o Marketplace
) where usuario_resposta_lojista is not null;




--2)Protocolos sem resposta por aging e lojista;
--PS1. Para obter o texto da resposta e toda a conversa basta acessar a tabela ac_admin.ATD_TICKET_LOG
--Ps2. O aging é calculado pelo campo tempo_aberto que é a diferença entre a data atual e a data de 
--     abertura do protocolo; ######AI PRECISA DEFINIR QUAL É O AGING PARA COMPLEMENTAR A QUERY#####;
--     e claro que protocolos que possuem uma data de fechamento não devem ser considerado, imagino eu
--     pelo menos
select distinct /*PARALLEL(20)*/ * from (
    select
        t.dat_created, --data da abertura do protocolo
        t.dat_closed, --data do fechamento do protocolo caso tenha sido fechado
        t.expiration_date,  --data máxima para atendimento
        sysdate - t.dat_created tempo_aberto, --tempo que ele está aberto (considerar apenas se a data fechamento não foi preenchida)
        t.cod_ticket, --identificação do protocolo
        t.id_order, --id do pedido no Marketplace (difere um pouquinho do pedido do FRONT)
        (select nam_ticket_subject from ac_admin.atd_ticket_subject where id_ticket_subject = t.id_ticket_subject) tipo, --está na tabela de domínio ac_admin.atd_ticket_subject
        (select nam_reason from ac_admin.atd_reason where id_reason = t.id_reason) motivo, --está na tabela de domínio ac_admin.atd_reason
        t.txt_email, --email do cliente do pedido
        case
            when t.cod_responsible = 0 then 'Marketplace'
            else (select store_name from ac_admin.ECAD_SELLER_STORE_INFO_VW where id = t.cod_responsible)
        end responsavel, --indica se o responsável atual é o Marketplace ou o lojista
        case
            when id_status = 1 then 'Aberto'
            when id_status = 2 then 'Fechado'
            when id_status = 6 then 'Sige'
            else 'N/A'
        end status, --indica os status atual do protocolo
        (select id_user from ac_admin.ATD_TICKET_LOG where id_ticket = t.id_ticket and rownum <= 1) usuario_resposta_lojista, --caso o lojista tenha dado alguma resposta, este campo vai mostrar o usuário que fez a primeira resposta; para pegar todas as respostas basta acesar os registros da tabela ac_admin.ATD_TICKET_LOG
        t.store_name, --nome do lojista do pedido
        (
            select nam_refund_reason from 
                ac_admin.ATD_REFUND_REASON rr 
            inner join
                ac_admin.ATD_ORDER_INFO oi
            on rr.id_refund_reason = oi.id_refund_reason
            where oi.id_ticket = t.id_ticket
            and rownum <= 1
        ) motivo_cancelamento_devolucao, --está na tabela de domínio ac_admin.ATD_REFUND_REASON (pode ter mais de uma linha porque quando lojista autoriza o cancelamento/devolução é adicionado mais um registro à esta tabela)
        case (
            select max(flg_approved) from 
                ac_admin.ATD_ORDER_INFO oi
            where oi.id_ticket = t.id_ticket
            group by id_ticket
        ) when 0 then 'Não'
          when 1 then 'Sim'
          else null
        end cancelamento_devolucao_aprovada, --indica se o cancelamento/devolução foi aprovado ou não; quando está nulo é porque o protocolo é de um outro tipo
        (select type_opener from ac_admin.ATD_TYPE_OPENER where id_type_opener = t.id_type_opener) origem_abertura,  --está na tabela de domínio ac_admin.ATD_TYPE_OPENER 
        (
        select S.nam_source 
        from 
            ac_admin.ATD_TICKET_COMPLAINT C
        inner join
            ac_admin.ATD_TICKET_COMPLAINT_SOURCE S
        on
            c.id_complaint_source = s.id_ticket_complaint_source
        where c.id_ticket = t.id_ticket and rownum <= 1
        ) origem_reclamacao_critica, ----está na tabela de domínio ac_admin.ATD_TICKET_COMPLAINT_SOURCE e indica caso tenha sido exposto a reclamação em redes sociais, Reclame aqui, etc.
        FLG_COMPLAINT em_ouvidoria -- 1 indica que sim, o protocolo está em ouvidoria e 0 indica que não está
    from 
        ac_admin.ATD_TICKET T  --tabela principal de tickets
    where id_store = 'NCMP'  --apenas registros do ambiente do Marketplace
        and ID_STATUS = 1  --apenas protocolos aberto
        and COD_OWNER != 0  --pega apenas os protocolos que o dono não é o Marketplace
        and COD_OWNER not in (select seller_id from ac_admin.NPC_CONF_VV) --pega apenas os protocolos que os lojistas não são Retira
        and t.cod_owner = t.COD_RESPONSIBLE --pega apenas os registros que o responsável é um lojista
) where usuario_resposta_lojista is null;




--3)Protocolos respondidos, por aging e lojista, texto resposta do lojista;
--PS1. Para obter o texto da resposta e toda a conversa basta acessar a tabela ac_admin.ATD_TICKET_LOG
--Ps2. O aging é calculado pelo campo tempo_aberto que é a diferença entre a data atual e a data de 
--     abertura do protocolo; ######AI PRECISA DEFINIR QUAL É O AGING PARA COMPLEMENTAR A QUERY#####;
--     e claro que protocolos que possuem uma data de fechamento não devem ser considerado, imagino eu
--     pelo menos
select distinct /*PARALLEL(20)*/ * from (
    select
        t.dat_created, --data da abertura do protocolo
        t.dat_closed, --data do fechamento do protocolo caso tenha sido fechado
        sysdate - t.dat_created tempo_aberto, --tempo que ele está aberto (considerar apenas se a data fechamento não foi preenchida)
        t.expiration_date,  --data máxima para atendimento
        t.cod_ticket, --identificação do protocolo
        t.id_order, --id do pedido no Marketplace (difere um pouquinho do pedido do FRONT)
        (select nam_ticket_subject from ac_admin.atd_ticket_subject where id_ticket_subject = t.id_ticket_subject) tipo, --está na tabela de domínio ac_admin.atd_ticket_subject
        (select nam_reason from ac_admin.atd_reason where id_reason = t.id_reason) motivo, --está na tabela de domínio ac_admin.atd_reason
        t.txt_email, --email do cliente do pedido
        case
            when t.cod_responsible = 0 then 'Marketplace'
            else (select store_name from ac_admin.ECAD_SELLER_STORE_INFO_VW where id = t.cod_responsible)
        end responsavel, --indica se o responsável atual é o Marketplace ou o lojista
        case
            when id_status = 1 then 'Aberto'
            when id_status = 2 then 'Fechado'
            when id_status = 6 then 'Sige'
            else 'N/A'
        end status, --indica os status atual do protocolo
        (select id_user from ac_admin.ATD_TICKET_LOG where id_ticket = t.id_ticket and rownum <= 1) usuario_resposta_lojista, --caso o lojista tenha dado alguma resposta, este campo vai mostrar o usuário que fez a primeira resposta; para pegar todas as respostas basta acesar os registros da tabela ac_admin.ATD_TICKET_LOG
        t.store_name, --nome do lojista do pedido
        (
            select nam_refund_reason from 
                ac_admin.ATD_REFUND_REASON rr 
            inner join
                ac_admin.ATD_ORDER_INFO oi
            on rr.id_refund_reason = oi.id_refund_reason
            where oi.id_ticket = t.id_ticket
            and rownum <= 1
        ) motivo_cancelamento_devolucao, --está na tabela de domínio ac_admin.ATD_REFUND_REASON (pode ter mais de uma linha porque quando lojista autoriza o cancelamento/devolução é adicionado mais um registro à esta tabela)
        case (
            select max(flg_approved) from 
                ac_admin.ATD_ORDER_INFO oi
            where oi.id_ticket = t.id_ticket
            group by id_ticket
        ) when 0 then 'Não'
          when 1 then 'Sim'
          else null
        end cancelamento_devolucao_aprovada, --indica se o cancelamento/devolução foi aprovado ou não; quando está nulo é porque o protocolo é de um outro tipo
        (select type_opener from ac_admin.ATD_TYPE_OPENER where id_type_opener = t.id_type_opener) origem_abertura,  --está na tabela de domínio ac_admin.ATD_TYPE_OPENER 
        (
        select S.nam_source 
        from 
            ac_admin.ATD_TICKET_COMPLAINT C
        inner join
            ac_admin.ATD_TICKET_COMPLAINT_SOURCE S
        on
            c.id_complaint_source = s.id_ticket_complaint_source
        where c.id_ticket = t.id_ticket and rownum <= 1
        ) origem_reclamacao_critica, ----está na tabela de domínio ac_admin.ATD_TICKET_COMPLAINT_SOURCE e indica caso tenha sido exposto a reclamação em redes sociais, Reclame aqui, etc.
        FLG_COMPLAINT em_ouvidoria -- 1 indica que sim, o protocolo está em ouvidoria e 0 indica que não está
    from 
        ac_admin.ATD_TICKET T  --tabela principal de tickets
    where id_store = 'NCMP'  --apenas registros do ambiente do Marketplace
        and ID_STATUS = 1  --apenas protocolos aberto
        and COD_OWNER != 0  --pega apenas os protocolos que o dono não é o Marketplace
        and COD_OWNER not in (select seller_id from ac_admin.NPC_CONF_VV) --pega apenas os protocolos que os lojistas não são Retira
        and t.cod_owner = t.COD_RESPONSIBLE --pega apenas os registros que o responsável é um lojista
) where usuario_resposta_lojista is not null;



--4)Protocolos por fila, status do protocolo; status do pedido e status do lojista (ativo/inativo);
--a) por fila
select count(1), fila from (
select /*PARALLEL(20)*/
    case
        when t.cod_responsible = 0 then 'Fila Marketplace'
        when t.cod_owner = t.cod_responsible then 'Fila Lojista'
    end fila
from 
    ac_admin.ATD_TICKET T  --tabela principal de tickets
where id_store = 'NCMP'  --apenas registros do ambiente do Marketplace
    and COD_OWNER not in (select seller_id from ac_admin.NPC_CONF_VV) --pega apenas os protocolos que os lojistas não são Retira
) group by fila;


--b) por status do protocolo
select count(1), status_protocolo 
from (
select /*PARALLEL(20)*/
    case
        when t.id_status = 1 and t.FLG_INTERVENTION = 1 then 'Crítico'
        when t.id_status = 1 and t.expiration_date <= sysdate then 'Atrasado'
        when t.id_status = 1 and t.id_priority = 2 then 'Em acompanhamento'
        when t.id_status = 1 then 'Aberto'
        when t.id_status = 2 then 'Fechado'
        when t.id_status = 6 then 'Sige - ignorar'
    end status_protocolo
from 
    ac_admin.ATD_TICKET T  --tabela principal de tickets
where id_store = 'NCMP'  --apenas registros do ambiente do Marketplace
    and COD_OWNER not in (select seller_id from ac_admin.NPC_CONF_VV) --pega apenas os protocolos que os lojistas não são Retira
) group by status_protocolo;


--c)status do lojista
select count(1), status_pedido 
from (
select /*PARALLEL(20)*/
    t.id_order,
    m.status status_pedido --status geral do pedido
from 
    ac_admin.ATD_TICKET T  --tabela principal de tickets
inner join
    ac_admin.ECAD_ORDER_MIS M  --tabela que é alimentada diretamente pelos dados do mongoDB
on
    m.order_id = t.id_order and m.store_id = t.id_store
where t.id_store = 'NCMP'  --apenas registros do ambiente do Marketplace
    and t.COD_OWNER not in (select seller_id from ac_admin.NPC_CONF_VV) --pega apenas os protocolos que os lojistas não são Retira
) group by status_pedido;



--d) status do lojista (ativo/inativo)
select count(1), status_loja 
from (
select /*PARALLEL(20)*/
    t.cod_ticket,
    t.cod_owner,
    case
    (select max(status) 
        from ac_admin.ecma_store_qualifier_site  --tabela que possui o status da loja
      where store_qualifier_id = t.cod_owner and store_id = t.id_store
    )
    when '1' then 'Ativa'
    when '0' then 'Inativa'
    when '3' then 'Inativa'
    else null
    end status_loja
from 
    ac_admin.ATD_TICKET T  --tabela principal de tickets
where t.id_store = 'NCMP'  --apenas registros do ambiente do Marketplace
    and t.COD_OWNER != 0
    and t.COD_OWNER not in (select seller_id from ac_admin.NPC_CONF_VV) --pega apenas os protocolos que os lojistas não são Retira
) where status_loja is not null
group by status_loja;



--5)Relatorio de pedidos cancelados com número da entrega, dados do cliente, lojista, dados do
--pagamento, valor da entrega, sku do item (sku Via), bandeira, quem cancelou (lojista ou marketplace);
--PS: contempla pedidos cancelados ou devolvidos
select distinct /*PARALLEL(20)*/ * from (
select
    t.id_order, --id do pedido no Marketplace
    case
        when oi.id_deliveries is null then 'Não recuperável'
        else oi.id_deliveries
    end id_entrega,   --id da entrega executada (muitos antigos não possuem)
    t.cod_ticket, --id do protocolo
    t.nam_customer nome_cliente, --nome do cliente
    t.txt_email email_cliente,  --email do cliente
    t.store_name,  --nome do lojista
    t.cod_owner,  --código da loja
    m.payment_name, --tipo do pagamento (se o pedido tem mais de um pagamento aqui não aparece)
    oi.val_freight valor_entrega,  --valor da entrega
    (select referenceid from ac_admin.ecma_sku_mp_related_sku_seller where sku_id = oi.id_sku and 
        store_id = t.id_store and referenceid is not null and rownum <= 1) sku_via, --pode vir nulo devido desassociação por exemplo
    oi.id_sku sku_mp, --sku do Marketplace
    m.site_name bandeira,  --Bandeira
    case oi.cod_owner
        when '0' then 'Marketplace'
        else 'Lojista'
    end reponsavel_cancelamento  --responsável pela execução
from
    ac_admin.ATD_TICKET T  --tabela principal de tickets
inner join
    ac_admin.ATD_ORDER_INFO OI  --tabela que armazena as trocas/devoluções executadas
on
    oi.id_ticket = t.id_ticket
left join
    ac_admin.ECAD_ORDER_MIS M
on
    m.order_id = t.id_order and m.store_id = t.id_store
where t.id_store = 'NCMP'  --apenas registros do ambiente do Marketplace
    and t.COD_OWNER not in (select seller_id from ac_admin.NPC_CONF_VV) --pega apenas os protocolos que os lojistas não são Retira
    and oi.flg_approved = 1 --apenas protocolos aprovados
);