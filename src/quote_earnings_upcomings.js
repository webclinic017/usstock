// find earnings_upcomings 
// and combine with yh_quote_curr and mapping_ticker_cik to the list of coming week import earnings
// and save it to earnings_upcomings
// run every Sunday 5am:
// cd /apps/fafa/pyx/alan
// python3 -c "from earnings_zacks import earnings_upcomings_batch as eub;eub()" )
// cat quote_earnings_upcomings.js | mongo

use ara;
DBQuery.shellBatchSize = 300;
db.earnings_upcomings_list.drop();
db.getCollection('yh_quote_curr').aggregate([
  {
    $lookup: {
      from: "earnings_upcomings",
      localField: "ticker",
      foreignField: "ticker", 
      as: "tkmatch"
    }
  } ,
  {
    $lookup: {
      from: "mapping_ticker_cik",
      localField: "ticker",
      foreignField: "ticker",
      as: "tkmapp"
    }
  } ,
  {
    $match: {
      tkmatch: {
         $not:{"$size": 0}
      },
      tkmapp: {
         $not:{"$size": 0}
      },
      "marketCap": {"$gt":1000000000} 
    }
  },
  {
    $project: {
      _id: 0  ,
      ticker:1  ,
      marketCap:1 ,
      shortName:1 ,
      "company":   { "$arrayElemAt" : [ "$tkmapp.company_cn" , 0 ] },
      "sector":   { "$arrayElemAt" : [ "$tkmapp.sector_cn" , 0 ] },
      "estimate": { "$arrayElemAt" : [ "$tkmatch.Estimate" , 0 ] },
      "pbdate": { "$arrayElemAt" : [ "$tkmatch.pbdate" , 0 ] }
    }
  },
  {
    $sort: {
      pbdate:1,
      marketCap:-1
    }
  }
]).forEach(function(doc){
   doc['pbdate']=NumberInt(doc['pbdate']);
   db.earnings_upcomings_list.insert(doc);
});
