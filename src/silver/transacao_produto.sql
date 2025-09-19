SELECT 
  IdTransacaoProduto AS idTransacaoProduto,
  IdTransacao AS idTransacao,
  IdProduto as idProduto,
  QtdeProduto AS nrQuantidadeProduto,
  VlProduto AS vlProduto
FROM bronze.upsell.transactions_product;