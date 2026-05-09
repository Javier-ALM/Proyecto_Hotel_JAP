select *
from {{ ref('silver_hotel_stg__reserva') }}
where fecha_checkin > fecha_checkout