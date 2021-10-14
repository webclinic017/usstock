use ara;
db.daily_single_stock.find({}).forEach(function(doc){
   db.daily_single_stock_archive.insert(doc);
});
db.daily_single_stock.deleteMany({});

