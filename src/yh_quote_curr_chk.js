use ara;
db.yh_quote_curr_chk.deleteMayn();
db.yh_quote_curr.find({pbdt:{$ne:null}},{ticker:1,pbdt:1}).forEach(function(doc){
   db.yh_quote_curr_chk.insert(doc);
});
