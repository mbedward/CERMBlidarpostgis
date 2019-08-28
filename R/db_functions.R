#' Run an SQL command on a selected database
#'
#' This function allows you to run arbitrary SQL commands on a selected database
#' via the active PostgreSQL server. It is an alternative to connecting to the
#' database via the RPostgreSQL package or similar. If the SQL commands are
#' provided as a character vector, they are run by first writing them to a
#' temporary file which is passed to the command line program \code{psql.exe}.
#' Multiple commands can be provided in concatenated form as either a
#' one-element character vector, or as a multi-element vector. In both cases,
#' each command must be followed by a semicolon as per normal SQL syntax (but
#' see Details for an exception to this rule in the case of special \code{psql}
#' commands).
#'
#' The \code{psql} command line program installed with PostgreSQL supports a
#' set of special commands in addition to standard SQL statements. For example,
#' \code{'\d mytable'} will return a description of the specified table while
#' \code{'\dt'} will list all tables in the database. These commands can be
#' issued via the \code{pg_sql} function and do not require a following
#' semi-colon. Note however that R will require the leading backslash to be
#' 'escaped', e.g. \code{'\\dt'} otherwise obscure error messages will result.
#'
#' @param command A valid SQL command as a character string or \code{glue}
#'   object. Can also be a special \code{psql} command (see Details). Not
#'   required if the \code{file} argument is used to read commands from a file.
#'
#' @param file The path and name of a file containing the SQL command(s) to
#'   run. Ignored if \code{command} is provided.
#'
#' @param dbname The name of an existing PostgreSQL / Postgis database (e.g.
#'   \code{'cermb_lidar'}). Can also be an empty string for general commands
#'   such as \code{'CREATE DATABASE foo;'}.
#'
#' @param username Name of the user (default: 'postgres').
#'
#' @param quiet If \code{TRUE} the output from the psql helper application is
#'   returned invisibly; if \code{FALSE} (default) the output is returned
#'   explicitly.
#'
#' @return Text of the database status message after running the command.
#'
#' @examples
#' \dontrun{
#' # Must do this once at the start of an R session
#' Sys.setenv(PGPASSWORD = "cermb")
#'
#' pg_sql("SELECT COUNT(*) AS NRECS FROM FOO;", dbname = "mydb")
#' }
#'
#' @export
#'
pg_sql <- function(command, file = NULL, dbname = "",
                   username = "postgres", quiet = TRUE) {

  PSQL <- .get_runtime_setting("PSQL")
  if (is.null(PSQL))
    stop("Runtime settings not ready. Have you called check_postgis()?")

  command <- .parse_command(command)
  if (!is.null(command)) {
    fsql <- tempfile(pattern = "sql", fileext = ".sql")
    cat(command, file = fsql)
  } else {
    if (!file.exists(file)) stop("No command provided so expected a file name")
    fsql <- file
  }

  dbarg <- ifelse(dbname == "", "", glue::glue('-d {dbname}'))
  args <- glue::glue('{dbarg} -U {username} -f {fsql}')
  out <- system2(command = PSQL, args = args, stdout = TRUE)

  unlink(fsql)

  if (quiet)
    invisible(out)
  else
    out
}


# Helper for pg_sql function.
# Returns NULL if command is empty or only white space.
# Flattens multi-element character vectors of commands to a single element.
#
.parse_command <- function(command) {
  command <- stringr::str_trim(command)
  if (is.na(command) || length(command) == 0)
    NULL
  else {
    if (length(command > 1)) {
      command <- stringr::str_subset(command, "[^\\s]")
      command <- stringr::str_trim( paste(command, collapse = " ") )
    }

    if (nchar(command) == 0) NULL
    else command
  }
}


#' Create the point counts table if it does not already exist
#'
#' This function sets up a table to store multi-band rasters of integer point
#' counts within voxels. The table has two columns: \code{rid}: an
#' auto-incremented integer record number; \code{rast} raster tiles (aka chips)
#' stored in binary format. There is no field explicitly linking raster records
#' to records in other tables (e.g. the LAS metadata table) since, although
#' the tiles in the database may initially correspond to individual LAS files,
#' this can change if rasters are re-tiled or unioned to optimize queries.
#'
#' @param dbname The name of an existing PostgreSQL / Postgis database
#'   (e.g. \code{'cermb_lidar'}).
#'
#' @param tablename Name of the raster table to hold point counts in the form
#'   \code{'schema.tablename'}. Defaults to \code{'rasters.point_counts'}.
#'
#' @param username Name of the user (default: 'postgres').
#'
#' @return Invisibly returns a list with the following elements:
#'   \describe{
#'     \item{status}{An integer code: 0 for success (as per \code{system});
#'       1 for skipped (the table already existed); or -1 for error.}
#'     \item{msg}{Message returned by the database.}
#'   }
#'
#' @export
#'
db_create_counts_table <- function(dbname,
                                   tablename = "rasters.point_counts",
                                   username = "postgres") {

  command <- glue::glue("CREATE TABLE IF NOT EXISTS \\
                      {tablename} \\
                      (rid serial primary key, rast raster);")

  out <- pg_sql(command, dbname = dbname, username = username)

  status <-
    if (any(stringr::str_detect(out, "ERROR"))) -1
    else if(any(stringr::str_detect(out, "NOTICE.+skipping"))) 1
    else 0

  invisible( list(status = status, msg = out) )
}


#' Create the LAS metadata table if it does not already exist
#'
#' Each record of the metadata table holds summary information derived from an
#' individual LAS file including source file, capture date and times, and point
#' counts and average point density. The table also has a geometry column for
#' the bounding rectangle of each LAS image.
#'
#' @param dbname The name of an existing PostgreSQL / Postgis database
#'   (e.g. \code{'cermb_lidar'}).
#'
#' @param tablename Name of the table to hold metadata and las tile bounding
#'   polygons in the form \code{'schema.tablename'}. Defaults to
#'   \code{'vectors.las_metadata'}.
#'
#' @param username Name of the user (default: 'postgres').
#'
#' @return Invisibly returns a list with the following elements:
#'   \describe{
#'     \item{status}{An integer code: 0 for success (as per \code{system});
#'       1 for skipped (the table already existed); or -1 for error.}
#'     \item{msg}{Message returned by the database.}
#'   }
#'
#' @export
#'
db_create_metadata_table <- function(dbname,
                                     tablename = "vectors.las_metadata",
                                     username = "postgres") {
  command <- glue::glue(
    "CREATE TABLE IF NOT EXISTS \\
    {tablename} (\\
    id serial PRIMARY KEY, \\
    filename text NOT NULL, \\
    capture_start timestamp with time zone NOT NULL, \\
    capture_end timestamp with time zone NOT NULL, \\
    area double precision NOT NULL, \\
    point_density double precision NOT NULL, \\
    npts_ground integer NOT NULL, \\
    npts_veg integer NOT NULL, \\
    npts_building integer NOT NULL, \\
    npts_water integer NOT NULL, \\
    npts_other integer NOT NULL, \\
    npts_total integer NOT NULL, \\
    nflightlines integer NOT NULL, \\
    bounds geometry(Polygon, 3308) NOT NULL);" )

  out <- pg_sql(command, dbname = dbname, username = username)

  status <-
    if (any(stringr::str_detect(out, "ERROR"))) -1
  else if(any(stringr::str_detect(out, "NOTICE.+skipping"))) 1
  else 0

  invisible( list(status = status, msg = out) )
}


#' Import a raster of point counts and its metadata into the database
#'
#' @param las.path The path and filename of the LAS source file. This can be
#'   an uncompressed (\code{.las}) or compressed \code{.laz} or \code{.zip}
#'   file.
#'
#' @param dbname The name of an existing PostgreSQL / Postgis database
#'   (e.g. \code{'cermb_lidar'}).
#'
#' @param counts.tablename The name of the table in which to store the raster of
#'   point counts within vertical layers. Should be in the form
#'   \code{'schema.table'}. Defaults to \code{'rasters.point_counts'}.
#'
#' @param metadata.tablename The name of the table in which to store the summary
#'   data and bounding polygon for the LAS image. Should be in the form
#'   \code{'schema.table'}. Defaults to \code{'vectors.las_metadata'}.
#'
#' @param epsg.code The EPSG code for the coordinate reference system to apply
#'   to data. The point cloud will be re-projected as necessary, prior to
#'   deriving point counts and other data. The default EPSG code is 3308
#'   (New South Wales Lambert projection / GDA94).
#'
#' @export
#'
db_import_las <- function(las.path,
                          dbname,
                          counts.tablename = "rasters.point_counts",
                          metadata.tablename = "vectors.las_metadata",
                          epsg.code = 3308) {

  message("Reading data and normalizing point heights")
  las <- prepare_tile(las.path)

  message("Reprojecting point cloud and removing any flight line overlap")
  las <- las %>%
    reproject_tile(epsg.code) %>%
    remove_flightline_overlap()

  message("Importing metadata")
  db_load_tile_metadata(las, fname,
                        dbname = DB,
                        tablename = metadata.tablename)

  message("Importing point counts for strata")
  counts <- get_stratum_counts(las, StrataCERMB)

  pg_load_raster(counts, epsg = 3308,
                 dbname = dbname,
                 tablename = counts.tablename,
                 tilew = ncol(counts),
                 tileh = nrow(counts))
}


#' Import a raster into the database
#'
#' @param r The raster to import.
#'
#' @param label A label to use to construct a file name for writing the raster
#'   to disk prior to import. This will be stored in the 'filename' column of
#'   the raster table.
#'
#' @param epsg EPSG code for the raster projection.
#'
#' @param dbname The name of an existing PostgreSQL / Postgis database
#'   (e.g. \code{'cermb_lidar'}).
#'
#' @param tablename Name of the raster table in the form \code{'schema.tablename'}.
#'
#' @param replace If \code{TRUE} and the table exists, it will be dropped and
#'   recreated. If \code{FALSE} (default) the data will be appended to an
#'   existing table if present, otherwise a new table will be created.
#'
#' @param tilew Width (pixels) of raster tiles as stored in the database
#'   table. Refer to PostGIS documentation for an explanation. If \code{NULL}
#'   (default) this will be set to the width of the input raster.
#'
#' @param tileh height (pixels) of raster tiles as stored in the database
#'   table. Refer to PostGIS documentation for an explanation. If \code{NULL}
#'   (default) this will be set to the height of the input raster.
#'
#' @param flags Other options to be passed to the PostgreSQL raster2pgsql
#'   program. Default is \code{"-M -Y"} to run vacuum analyzed (-M) and
#'   use COPY instead of INSERT statements for speed (-Y). See
#'   raster2pgsql documentation for details of other options.
#'
#' @param username Name of the user (default: 'postgres').
#'
#' @export
#'
pg_load_raster <- function(r, epsg,
                           dbname, tablename,
                           replace = FALSE,
                           tilew = NULL, tileh = NULL,
                           flags = "-M -Y",
                           username = "postgres") {

  PSQL <- .get_runtime_setting("PSQL")
  R2P <- .get_runtime_setting("R2P")
  if (is.null(PSQL) || is.null(R2P))
    stop("Runtime settings not ready. Have you called check_postgis()?")

  is.tbl <- pg_table_exists(dbname, tablename, username)
  if (replace) {
    in.mode <- "-d"
  } else if (is.tbl) {
    in.mode <- "-a"
  } else {
    in.mode <- "-c"
  }

  ftif <- tempfile(pattern = "counts", fileext = ".tif")
  fsql <- tempfile(pattern = "sql", fileext = ".txt")

  raster::writeRaster(r, filename = ftif,
                      format = "GTiff",
                      overwrite = TRUE)

  if (is.null(tilew)) tilew <- ncol(r)
  if (is.null(tileh)) tileh <- nrow(r)

  stopifnot(tilew > 0, tileh > 0)

  args <- glue::glue('{in.mode} -s {epsg} -t {tilew}x{tilew} {flags} {ftif} {tablename}')
  system2(command = R2P, args = args, stdout = fsql)

  args <- glue::glue('-d {dbname} -U {username} -f {fsql}')
  system2(command = PSQL, args = args)

  unlink(ftif)
  unlink(fsql)
}


#' Import LAS tile metadata into the database
#'
#' @param las A LAS object.
#'
#' @param filename Path or filename from which the LAS object was read.
#'
#' @param dbname The name of an existing PostgreSQL / Postgis database
#'   (e.g. \code{'cermb_lidar'}).
#'
#' @param tablename Name of the raster table in the form \code{'schema.tablename'}.
#'
#' @param username Name of the user (default: 'postgres').
#'
#' @export
#'
db_load_tile_metadata <- function(las, filename,
                                  dbname, tablename,
                                  username = "postgres") {

  PSQL <- .get_runtime_setting("PSQL")
  R2P <- .get_runtime_setting("R2P")
  if (is.null(PSQL) || is.null(R2P))
    stop("Runtime settings not ready. Have you called check_postgis()?")

  if (!pg_table_exists(dbname, tablename, username)) {
    msg <- glue::glue("Table {tablename} not found in database {dbname}")
    stop(msg)
  }


  epsgcode <- lidR::epsg(las)

  filename <- .file_from_path(filename)
  scantimes <- CERMBlidar::get_scan_times(las, by = "all")

  pcounts <- get_point_counts(las)
  ptotal <- Reduce(sum, pcounts)

  nflightlines <- length(unique(las@data$flightlineID))

  bounds <- get_las_bounds(las, "sf")
  wkt <- sf::st_as_text(bounds)
  area <- sf::st_area(bounds)

  tformat <- function(timestamp) format(timestamp, usetz = TRUE)

  command <- glue::glue(
    "insert into {tablename} \\
    (filename, capture_start, capture_end, \\
    area, \\
    point_density, \\
    npts_ground, npts_veg, npts_building, npts_water, npts_other, npts_total, \\
    nflightlines,
    bounds) \\
    values(\\
    '{filename}', \\
    '{tformat(scantimes[1,1])}', \\
    '{tformat(scantimes[1,2])}', \\
    {area}, \\
    {ptotal / area}, \\
    {pcounts$ground}, \\
    {pcounts$veg}, \\
    {pcounts$building}, \\
    {pcounts$water}, \\
    {pcounts$other}, \\
    {ptotal}, \\
    {nflightlines}, \\
    ST_GeomFromText('{wkt}', {epsgcode}) );
    ")

  pg_sql(command, dbname = dbname, username = username)
}


#' Check if a table exists in the given database
#'
#' The check is done by using the \code{psql.exe} helper program to retrieve
#' metadata for the table, and checking that something was found.
#'
#' @param dbname The name of an existing PostgreSQL / Postgis database
#'   (e.g. \code{'cermb_lidar'}).
#'
#' @param tablename Name of the table.
#'
#' @param username Name of the user (default: 'postgres').
#'
#' @return \code{TRUE} if the table was found; \code{FALSE} otherwise.
#'
#' @examples
#' \dontrun{
#' pg_table_exists("nsw_lidar", "postgis", "bega")
#' }
#'
#' @export
#'
pg_table_exists <- function(dbname, tablename, username = "postgres") {
  command <- glue::glue('\\d {tablename}')
  x <- pg_sql(command, dbname = dbname)

  ptn <- glue::glue('table.+{tablename}')
  stringr::str_detect(tolower(x[1]), ptn)
}


#' Get counts for point classes
#'
#' Gets the number of points in classes: ground, veg, building, water, and
#' other.
#'
#' @param las A LAS object.
#'
#' @return A named list of the count of points in each of the following
#' classes: ground, veg, building, water, other.
#'
#' @export
#'
get_point_counts <- function(las) {
  x <- table(las@data$Classification)
  x <- data.frame(code = names(x), n = as.numeric(x), stringsAsFactors = FALSE)

  x$class <- sapply(x$code, switch,
                    '2' = "ground",
                    '3' = "veg",
                    '4' = "veg",
                    '5' = "veg",
                    '6' = "building",
                    '9' = "water",
                    "other")

  x <- tapply(x$n, x$class, sum)

  all.classes <- c("ground", "veg", "building", "water", "other")
  to.add <- setdiff(all.classes, names(x))
  n <- length(to.add)
  if (n > 0) {
    x2 <- integer(n)
    names(x2) <- to.add
    x <- c(x, x2)
  }

  as.list(x)
}


#' Get the bounding rectangle of a LAS tile
#'
#' This function constructs a polygon based on the minimum and maximum X and Y
#' ordinates in the LAS data table, and returns it as either a WKT (Well Known
#' Text) string specifier or an \code{sf} polygon object.
#'
#' @param las A LAS object.
#'
#' @param type Either \code{'wkt'} (default) to return a WKT text string or
#'   \code{'sf'} to return a polygon object.
#'
#' @return The bounding polygon in the format specified by the \code{type}
#'   argument.
#'
#' @export
#'
get_las_bounds <- function(las, type = c("wkt", "sf")) {
  type <- match.arg(type)
  outfn <- ifelse(type == "wkt", sf::st_as_text, base::identity)

  xys <- c(range(las@data$X), range(las@data$Y))
  ii <- c(1,3, 1,4, 2,4, 2,3, 1,3)
  v <- matrix(xys[ii], ncol = 2, byrow = TRUE)
  outfn( sf::st_polygon(list(v)) )
}


# Extract filename and extension from a path
.file_from_path <- function(path) {
  stringr::str_extract(path, "[\\w\\.\\-]+$")
}

# Remove extension from filename
.file_remove_extension <- function(filename) {
  filename <- filename[1]
  ext <- "\\.\\w+$"

  if (stringr::str_detect(filename, ext))
    pos <- stringr::str_locate(filename, ext)[1,1] - 1
  else
    pos <- -1

  stringr::str_sub(filename, 1, pos)
}
