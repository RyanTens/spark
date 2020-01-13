package com.tens.sql

class SqlApp {
  def main(args: Array[String]): Unit = {

  }
}

/*
//需求
计算各个区域前三大热门商品，并备注上每个商品在主要城市中的分布比例，超过两个城市用其他显示。
例如:
地区	商品名称		点击次数	城市备注
华北	商品A		100000	北京21.2%，天津13.2%，其他65.6%
华北	商品P		80200	北京63.0%，太原10%，其他27.0%
华北	商品M		40000	北京63.0%，太原10%，其他27.0%
东北	商品J		92000	大连28%，辽宁17.0%，其他 55.0%

//1.先把需要的字段查出来 t1
select
  ci.area,
  pi.product_name,
  uv.click_product_id,
  ci.city_name
from user_visit_action uv
join product_info pi on uv.click_product_id=pi.product_id
join city_info ci on uv.city_id=ci.city_id

//2.按照地区和商品进行分区聚合 t2
select
  area,
  product_name,
  count(*) click_count
from t1
group by area,product_name

//3.每个地区按照点击总数降序排名 开窗 t3
select
  *,
  rank() over(partition by area order by click_count desc) rk
from t2

//4.取前三
select
  area,
  product_name,
  click_count
from t3
where rk <= 3
 */