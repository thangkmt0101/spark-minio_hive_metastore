


select 
'{{year_utc_7}}-{{month_utc_7}}-{{day_utc_7}}' ||  '00' as datetime_id,
'{{year_utc_7}}-{{month_utc_7}}-{{day_utc_7}}' as date
,dts.toll_type
,dts.boo, dts.cycle_id, dts.cycle_name
,fttd.tickettype ticket_type,fttd.prioritytype price_ticket_type
,count(distinct fttd.transporttransactionid ) qty
,sum(fttd.paidamount) amount
,'before_adj_amount' type
from ice.gold.fact_transporttransactiondetails fttd
inner join ice.gold.vw_toll_stage dts on fttd.fromtollid = dts.toll_in_id and fttd.totollid = dts.toll_out_id
WHERE (
    {% for day_range in day_ranges %}
    (A.year = '{{ day_range.year }}'
     AND A.month = '{{ day_range.month }}'
     AND A.day = '{{ day_range.day }}'
     AND A.hour BETWEEN '{{ day_range.hour_start }}' AND '{{ day_range.hour_end }}')
    {% if not loop.last %} OR {% endif %}
    {% endfor %}
  )
group by fttd.checkindatetime, dts.toll_type,dts.boo, dts.cycle_id, dts.cycle_name,fttd.tickettype,fttd.prioritytype

union all

select 
'{{year_utc_7}}-{{month_utc_7}}-{{day_utc_7}}' ||  '00' as datetime_id,
'{{year_utc_7}}-{{month_utc_7}}-{{day_utc_7}}' as date
,dts.toll_type
,dts.boo, dts.cycle_id, dts.cycle_name
,fttd.tickettype ticket_type,fttd.prioritytype price_ticket_type
,count(distinct fttd.transporttransactionid ) qty
,sum(fttd.paidamount) amount
,'adjustment_amount' type
from ice.gold.fact_transporttransactiondetails fttd
inner join ice.gold.vw_toll_stage dts on fttd.fromtollid = dts.toll_in_id and fttd.totollid = dts.toll_out_id
where exists (select * from ice.gold.fact_chargetransactiondetails fctt where fttd.transporttransactionid = fctt.transporttransactionid)
and (
    {% for day_range in day_ranges %}
    (A.year = '{{ day_range.year }}'
     AND A.month = '{{ day_range.month }}'
     AND A.day = '{{ day_range.day }}'
     AND A.hour BETWEEN '{{ day_range.hour_start }}' AND '{{ day_range.hour_end }}')
    {% if not loop.last %} OR {% endif %}
    {% endfor %}
  )
group by fttd.checkindatetime, dts.toll_type,dts.boo, dts.cycle_id, dts.cycle_name,fttd.tickettype,fttd.prioritytype

union all

select 
'{{year_utc_7}}-{{month_utc_7}}-{{day_utc_7}}' ||  '00' as datetime_id,
'{{year_utc_7}}-{{month_utc_7}}-{{day_utc_7}}' as date
,dts.toll_type
,dts.boo, dts.cycle_id, dts.cycle_name
,fttd.tickettype ticket_type,fttd.prioritytype price_ticket_type
,count(distinct fttd.transporttransactionid ) qty
,sum(fttd.paidamount) amount
,'after_adj_amount' type
from ice.gold.fact_transporttransactiondetailfinals fttd
inner join ice.gold.vw_toll_stage dts on fttd.fromtollid = dts.toll_in_id and fttd.totollid = dts.toll_out_id
where (
    {% for day_range in day_ranges %}
    (A.year = '{{ day_range.year }}'
     AND A.month = '{{ day_range.month }}'
     AND A.day = '{{ day_range.day }}'
     AND A.hour BETWEEN '{{ day_range.hour_start }}' AND '{{ day_range.hour_end }}')
    {% if not loop.last %} OR {% endif %}
    {% endfor %}
  )
group by fttd.checkindatetime, dts.toll_type,dts.boo, dts.cycle_id, dts.cycle_name,fttd.tickettype,fttd.prioritytype



