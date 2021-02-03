from database import add_default_date_value
from proceso import insert_proceso_target
from ubigeo import insert_ubigeo_target
from cargo import insert_cargo_target
from organizacion_politica import insert_organizacion_target
from estudio import insert_estudio_target
from institucion import insert_institucion_target
from candidato import insert_candidato_target, update_candidato_target
from candidato_judicial import insert_candidato_judicial_target
from candidato_estudio import insert_candidato_estudio_target
from candidato_experiencia import insert_candidato_experiencia_target
from indicador import insert_indicador_target
from indicador_categoria import insert_indicador_categoria_target
from indicador_categoria_organizacion import insert_indicador_categoria_organizacion_target
from indicador_categoria_candidato import insert_indicador_categoria_candidato_target
from candidato_bien_mueble import insert_candidato_bien_muebles, update_candidato_bien_muebles
from candidato_bien_inmueble import insert_candidato_bien_inmuebles
from candidato_ingreso import insert_candidato_ingresos


add_default_date_value()

insert_proceso_target()
insert_ubigeo_target()
insert_cargo_target()
insert_organizacion_target()
insert_estudio_target()
insert_institucion_target()
insert_candidato_target()
insert_candidato_judicial_target()
insert_candidato_estudio_target()
insert_candidato_experiencia_target()
update_candidato_target()
insert_candidato_bien_muebles()
insert_candidato_bien_inmuebles()
insert_candidato_ingresos()
update_candidato_bien_muebles()

insert_indicador_target()
insert_indicador_categoria_target()
insert_indicador_categoria_organizacion_target()
insert_indicador_categoria_candidato_target()
