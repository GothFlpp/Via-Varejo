select /*+ PARALLEL(20)*/ distinct sku.sku_id SKU_MP, mp.sku_id_origin SKU_LOJISTA, mp.referenceid SKU_SITE, pr0.value PRECO_DE, pr1.value PRECO_POR,
bar.value bar_code, puda.value marca,
sl.quantity quantidade,
case when ss.status = 'Y' then 'SIM'
when ss.status = 'N' then 'NÃƒO' end "Ativo?"
, mis.shop_code, mis.fancy_name,
replace(regexp_substr(vw.category_name, '[^.>]*>'),'>','')
from ac_admin.ecad_order_mis mis
left join ac_admin.ecad_sku sku on mis.sku_id = sku.sku_id
left join ac_admin.ecad_product p on sku.product_id = p.product_id
left join ac_admin.ecma_sku_mp_related_sku_seller mp on sku.sku_id = mp.sku_id and sku.product_id = mp.product_id and mis.shop_code = mp.store_qualifier_id
left join ac_admin.ecad_nonmarket_structure non on p.product_id = non.product_id


left join AC_ADMIN.ECIA_CATEGORY_TREE_VW vw on non.level_id = vw.id
left join ac_admin.ecad_warehouse w on mp.store_qualifier_id = w.store_qualifier_id and mp.store_id = w.store_id
left join ac_admin.ecad_price pr0 on mis.sku_id = pr0.sku_id and mis.shop_code = pr0.store_qualifier_id and pr0.price_tp_id = '0'
left join ac_admin.ecad_price pr1 on mis.sku_id = pr1.sku_id and mis.shop_code = pr1.store_qualifier_id and pr1.price_tp_id = '1'
left join ac_admin.ecad_product_uda puda on mp.product_id = puda.product_id and puda.uda_id = '206508'
left join ac_admin.ecad_current_stock sl on mis.sku_id = sl.sku_id and w.warehouse_id = sl.warehouse_id
left join ac_admin.ecad_sku_bar_code bar on mp.sku_id = bar.sku_id
left join ac_admin.ecma_sku_store_status ss on mp.sku_id = ss.sku_id and mp.store_qualifier_id = ss.store_qualifier_id and ss.site_id = '3'
where mis.purchase_date >= sysdate - 30
and mis.shop_code not in (select cod3 from ac_admin.temp where cod1 = 'LOJAS_QUE_NAO_ENTRAM_EM_RELATORIOS')
and mis.delivery_id in (select max(delivery_id) from ac_admin.ecad_order_mis where order_id = mis.order_id)
and mis.sku_id > 0 and ss.sku_id > 0
--and mp.referenceid = '13639904'

host: ora_adprd.dc.nova
port: 1521
database: ADPRD 
username: SRVDATA
pwd: b1gd4t###drc2018