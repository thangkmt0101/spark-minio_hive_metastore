


select 
'{{year_utc_7}}-{{month_utc_7}}-{{day_utc_7}}' ||  '00' as datetime_id,
'{{year_utc_7}}-{{month_utc_7}}-{{day_utc_7}}' as date
,sum(case when vehicletypeid = 1 then count_vehicle end) loai_1
,sum(case when vehicletypeid = 2 then count_vehicle end) loai_2
,sum(case when vehicletypeid = 3 then count_vehicle end) loai_3
,sum(case when vehicletypeid = 4 then count_vehicle end) loai_4
,sum(case when vehicletypeid = 5 then count_vehicle end) loai_5
,0 monthly_quarterly_pass
,0 priority_vehicle
from(select fttdf.checkoutdatetime, dv.vehicletypeid
,count(distinct fttdf.vehicleid) count_vehicle

FROM ice.gold.fact_transporttransactiondetailfinals fttdf
inner join ice.gold.dim_vehicle dv on fttdf.vehicleid = dv.id 
WHERE (
    {% for day_range in day_ranges %}
    (A.year = '{{ day_range.year }}'
     AND A.month = '{{ day_range.month }}'
     AND A.day = '{{ day_range.day }}'
     AND A.hour BETWEEN '{{ day_range.hour_start }}' AND '{{ day_range.hour_end }}')
    {% if not loop.last %} OR {% endif %}
    {% endfor %}
  )
group by fttdf.checkoutdatetime, dv.vehicletypeid
)a

union all

select 
'{{year_utc_7}}-{{month_utc_7}}-{{day_utc_7}}' ||  '00' as datetime_id,
'{{year_utc_7}}-{{month_utc_7}}-{{day_utc_7}}' as date
,0 loai_1
,0 loai_2
,0 loai_3
,0 loai_4
,0 loai_5
,case when fttdf.prioritytype in (2,3) then count(distinct fttdf.vehicleid)  end monthly_quarterly_pass
,case when fttdf.prioritytype in (2,3) then count(distinct fttdf.vehicleid)   end priority_vehicle
FROM ice.gold.fact_transporttransactiondetailfinals fttdf
inner join ice.gold.dim_vehicle dv on fttdf.vehicleid = dv.id 
WHERE (
    {% for day_range in day_ranges %}
    (A.year = '{{ day_range.year }}'
     AND A.month = '{{ day_range.month }}'
     AND A.day = '{{ day_range.day }}'
     AND A.hour BETWEEN '{{ day_range.hour_start }}' AND '{{ day_range.hour_end }}')
    {% if not loop.last %} OR {% endif %}
    {% endfor %}
  )
group by  fttdf.prioritytype