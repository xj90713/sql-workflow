----DC|222181b9-10ea-4046-b26e-23beff240c7c|0 0 * * * ? *|客服:${staff_name}，员工中心座机号码:${office_phone}，股后台座机号码:${landline_number}，请确认号码一致。
select
    t1.staff_code,t1.staff_name,t1.office_phone,REPLACE(t2.landline_number,'-','') as landline_number
from
    customer_service.v_staff_basic_info t1
        left join hydrus_admin.sys_user t2 on t1.staff_code = t2.staff_code
where
    t1.staff_code in (
        select
            staff_code
        from
            customer_service.v_cust_service_info
        where
            first_dept_code=80000008 and staff_code <> 80000859
          and job_status = 1)
  and (t1.office_phone <> REPLACE(t2.landline_number,'-','') or t1.office_phone is null or t1.office_phone = '')