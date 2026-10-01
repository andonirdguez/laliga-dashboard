// =====================================================================
// Conexión Power BI -> CSV de data/gold en GitHub (sin gateway)
// =====================================================================
// 1) En Power BI Desktop: Obtener datos > Consulta en blanco > Editor avanzado.
// 2) Crea estas dos consultas (nombre exacto) y luego una consulta por tabla.
// 3) La URL base debe ser FIJA y la ruta ir en RelativePath: así el Service
//    permite el refresh programado sin gateway (credenciales: Anónimo).
// 4) Se tipa con cultura "en-US" porque los CSV usan punto decimal.
// =====================================================================

// ---- Consulta: pRepo  (parámetro; cambia TU_USUARIO) ----
"TU_USUARIO/laliga-dashboard/main/data/gold/" meta [IsParameterQuery = true, Type = "Text", IsParameterQueryRequired = true]

// ---- Consulta: fnGold ----
(archivo as text) as table =>
let
    Origen   = Web.Contents("https://raw.githubusercontent.com", [RelativePath = pRepo & archivo]),
    Csv      = Csv.Document(Origen, [Delimiter = ",", Encoding = 65001, QuoteStyle = QuoteStyle.Csv]),
    Cabecera = Table.PromoteHeaders(Csv, [PromoteAllScalars = true])
in
    Cabecera

// ---- Consulta: dim_equipo ----
let
    Fuente = fnGold("dim_equipo.csv"),
    Tipos  = Table.TransformColumnTypes(Fuente, {{"equipo_id", type text}, {"equipo", type text}}, "en-US")
in
    Tipos

// ---- Consulta: dim_temporada ----
let
    Fuente = fnGold("dim_temporada.csv"),
    Tipos  = Table.TransformColumnTypes(Fuente, {{"temporada", type text}, {"anio_inicio", Int64.Type},
                                                 {"es_temporada_actual", Int64.Type}}, "en-US")
in
    Tipos

// ---- Consulta: dim_fecha ----
let
    Fuente = fnGold("dim_fecha.csv"),
    Tipos  = Table.TransformColumnTypes(Fuente, {
        {"fecha_id", Int64.Type}, {"fecha", type date}, {"anio", Int64.Type}, {"mes", Int64.Type},
        {"orden_mes_temporada", Int64.Type}, {"num_dia_semana", Int64.Type},
        {"es_fin_de_semana", Int64.Type}, {"semana_iso", Int64.Type}}, "en-US")
in
    Tipos

// ---- Consulta genérica para el resto de tablas (fact_*, dim_jugador, meta) ----
// Tipado automático: columnas *_id, texto y fechas conocidas; el resto, número decimal.
let
    Tabla   = "fact_partido.csv",   // <- cambia por cada tabla
    Fuente  = fnGold(Tabla),
    Texto   = {"partido_id", "jugador_id", "equipo_id", "equipo_local_id", "equipo_visitante_id",
               "rival_id", "temporada", "resultado", "posicion_grupo", "forma_ultimos5",
               "jugador", "posicion", "nacionalidad", "pie", "imagen_url", "tabla",
               "actualizado_utc", "fuentes_disponibles", "tm_player_id", "temporadas"},
    Fechas  = {"fecha", "fin_contrato"},
    Enteros = {"fecha_id"},
    Cols    = Table.ColumnNames(Fuente),
    Reglas  = List.Transform(Cols, each
                if List.Contains(Texto, _) then {_, type text}
                else if List.Contains(Fechas, _) then {_, type date}
                else if List.Contains(Enteros, _) then {_, Int64.Type}
                else {_, type number}),
    Tipos   = Table.TransformColumnTypes(Fuente, Reglas, "en-US")
in
    Tipos
