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
#' @param dbsettings A named list of database connection settings returned
#'   by \code{db_connect_postgis} or \code{db_create_postgis}.
#'
#' @param command A valid SQL command as a character string or \code{glue}
#'   object. Can also be a special \code{psql} command (see Details). Not
#'   required if the \code{file} argument is used to read commands from a file.
#'
#' @param file The path and name of a file containing the SQL command(s) to
#'   run. Ignored if \code{command} is provided.
#'
#' @param quiet If \code{TRUE} the output from the psql helper application is
#'   returned invisibly; if \code{FALSE} (default) the output is returned
#'   explicitly.
#'
#' @return Text of the database status message after running the command.
#'
#' @seealso \code{\link{db_connect_postgis}} \code{\link{db_create_postgis}}
#'
#' @examples
#' \dontrun{
#' Sys.setenv(PGPASSWORD = "cermb")
#'
#' dbsettings <- db_connect_postgis("cermb_lidar")
#'
#' pg_sql(dbsettings, "SELECT COUNT(*) AS NRECS FROM FOO;")
#' }
#'
#' @export
#'
pg_sql <- function(dbsettings, command = NULL, file = NULL, quiet = TRUE) {

  command <- .parse_command(command)

  if (!is.null(command)) {
    fsql <- tempfile(pattern = "sql", fileext = ".sql")
    cat(command, file = fsql)
  } else {
    if (!file.exists(file)) stop("No command provided so expected a file name")
    fsql <- file
  }

  args <- glue::glue('-d {dbsettings$DBNAME} -U {dbsettings$USERNAME} -f {fsql}')
  out <- system2(command = dbsettings$PSQL, args = args, stdout = TRUE)

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


#' Import a raster of point counts and its metadata into the database
#'
#' First, the LAS tile is read from disk and the function checks that it is
#' located in one of the map zones supported by the database (each zone's
#' data is stored in a separate schema). Next, point heights are normalized,
#' any overlap between flight lines is removed, and the counts of vegetation,
#' ground and water points are rasterized as vertical layers defined in
#' \code{CERMBlidar::CERMBstrata}. Finally, the point count data are imported
#' into the database along with meta-data for tile extent, point density, etc.
#'
#' @param dbsettings A named list of database connection settings returned
#'   by \code{\link{db_connect_postgis}}.
#'
#' @param las.path The path and filename of the LAS source file. This can be
#'   an uncompressed (\code{.las}) or compressed \code{.laz} or \code{.zip}
#'   file.
#'
#' @param dem.path The path and filename of a DEM raster file to use for
#'   normalizing point heights. This can be an uncompressed (\code{.asc} or
#'   \code{.tif}) file, or a compressed \code{.zip} file. If \code{NULL}
#'   (default), then point heights will be normalized via triangulation of
#'   the LAS ground points using the \code{\link[lidR]{tin}} algorithm
#'   with \code{\link[lidR]{lasnormalize}}.
#'
#' @export
#'
db_import_las <- function(dbsettings,
                          las.path,
                          dem.path = NULL) {

  message("Reading data and normalizing point heights")

  if (is.null(dem.path))
    las <- CERMBlidar::prepare_tile(las.path, normalize.heights = "tin")
  else
    las <- CERMBlidar::prepare_tile(las.path, normalize.heights = dem.path)

  message("Removing any flight line overlap")
  las <- CERMBlidar::remove_flightline_overlap(las)

  message("Importing LAS metadata")
  metadata.id <- db_load_tile_metadata(dbsettings,
                                       las, las.path)

  message("Importing point counts for strata")
  db_load_stratum_counts(dbsettings,
                         las, metadata.id)

  message("Importing building points")
  db_load_building_points(dbsettings,
                          las, metadata.id)
}


#' Check if one or more LAS files have been imported into the database
#'
#' When a LAS image is imported, the name of the file from which it was read is
#' recorded in the \code{lidar.las_metadata} table of the database. This
#' function queries that table to see which, if any, of the names provided
#' in the \code{filenames} argument are present in that table.
#'
#' @param dbsettings A named list of database connection settings returned
#'   by \code{db_connect_postgis} or \code{db_create_postgis}.
#'
#' @param filenames A character vector of one or more file names. These can be
#'   full paths or file names with or without extensions. File extensions
#'   (e.g. \code{.las; .LAZ; .zip}) and are ignored for comparison purposes, as
#'   is case.
#'
#' @seealso \code{\link{db_connect_postgis}} \code{\link{db_create_postgis}}
#'
#' @return A logical vector with the input file names as element names and
#'   \code{TRUE} values indicating files that have already been imported.
#'
#' @export
#'
db_lasfile_imported <- function(dbsettings, filenames) {
  if (!pg_table_exists(dbsettings, dbsettings$TABLE_METADATA))
    stop("No metadata table - is this a properly initialized database?")

  sapply(filenames, function(fname) {
    fname <- .file_remove_extension( .file_from_path(fname) )

    p <- dbsettings$POOL

    command <- glue::glue("SELECT COUNT(*) AS nrecs
                           FROM {dbsettings$TABLE_METADATA}
                           WHERE filename ILIKE '{fname}%'")

    x <- pool::dbGetQuery(p, command)
    x$nrecs[1] > 0
  })
}


#' Import a raster into the database
#'
#' @param dbsettings A named list of database connection settings returned
#'   by \code{db_connect_postgis} or \code{db_create_postgis}.
#'
#' @param r The raster to import.
#'
#' @param label A label to use to construct a file name for writing the raster
#'   to disk prior to import. This will be stored in the 'filename' column of
#'   the raster table.
#'
#' @param epsg EPSG code for the raster projection.
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
#' @seealso \code{\link{db_connect_postgis}} \code{\link{db_create_postgis}}
#'
#' @export
#'
pg_load_raster <- function(dbsettings,
                           r, epsg,
                           tablename,
                           replace = FALSE,
                           tilew = NULL, tileh = NULL,
                           flags = "-M -Y") {

  is.tbl <- pg_table_exists(dbsettings, tablename)
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
  system2(command = dbsettings$R2P, args = args, stdout = fsql)

  args <- glue::glue('-d {dbsettings$DBNAME} -U {dbsettings$USERNAME} -f {fsql}')
  system2(command = dbsettings$PSQL, args = args)

  unlink(ftif)
  unlink(fsql)
}


#' Import LAS tile metadata into the database
#'
#' @param dbsettings A named list of database connection settings returned
#'   by \code{db_connect_postgis} or \code{db_create_postgis}.
#'
#' @param las A LAS object.
#'
#' @param filename Path or filename from which the LAS object was read.
#'
#' @return The integer value of the \code{'id'} field for the newly created
#'   database record.
#'
#' @seealso \code{\link{db_connect_postgis}} \code{\link{db_create_postgis}}
#'
#' @export
#'
db_load_tile_metadata <- function(dbsettings,
                                  las, filename) {

  epsgcode <- lidR::epsg(las)
  schema <- .get_schema_for_epsg(dbsettings, epsgcode)

  tblname <- glue::glue("{schema}.{dbsettings$TABLE_METADATA}")

  if (!pg_table_exists(dbsettings, tblname)) {
    msg <- glue::glue("Table {tblname} not found in database {dbname}")
    stop(msg)
  }

  filename <- .file_from_path(filename)
  filename <- .file_remove_extension(filename)
  scantimes <- CERMBlidar::get_scan_times(las, by = "all")

  pcounts <- CERMBlidar::get_class_frequencies(las)
  ptotal <- Reduce(sum, pcounts)

  nflightlines <- length(unique(las@data$flightlineID))

  bounds <- CERMBlidar::get_las_bounds(las, "sf")
  wkt <- sf::st_as_text(bounds)
  area <- sf::st_area(bounds)

  command <- glue::glue(
    "insert into {tblname} \\
    (filename, capture_start, capture_end, \\
    area, \\
    point_density, \\
    npts_ground, npts_veg, npts_building, npts_water, npts_other, npts_total, \\
    nflightlines,
    bounds) \\
    values(\\
    '{filename}', \\
    '{.tformat(scantimes[1,1])}', \\
    '{.tformat(scantimes[1,2])}', \\
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

  p <- dbsettings$POOL
  pool::dbExecute(p, command)

  # Return id value of the record just created
  res <- pool::dbGetQuery(p, glue::glue("select id from {tblname} \\
                                        where filename = '{filename}'"))

  res$id[1]
}


#' Rasterize and import points counts
#'
#' @export
#'
db_load_stratum_counts <- function(dbsettings, las, metadata.id) {

  counts <- CERMBlidar::get_stratum_counts(las, CERMBlidar::StrataCERMB)

  schema <- .get_schema_for_epsg(dbsettings, lidR::epsg(las))
  tmp.tblname <- paste(schema, "tmp_load", sep = ".")

  # Load point counts for this tile into the temp table
  pg_load_raster(dbsettings,
                 counts,
                 epsg = lidR::epsg(las),
                 tablename = tmp.tblname,
                 tilew = ncol(counts),
                 tileh = nrow(counts))


  # Merge the new and existing rasters
  ## code to prepare `MergeImport` dataset goes here

  cmd <- glue::glue("
    -- Copy data into point_counts table
    insert into {schema}.point_counts (meta_id, rast)
    select {metadata.id} as meta_id, rast from {tmp.tblname};

    -- Identify existing rasters that overlap the new data
    create or replace view {schema}.overlaps as
      select pcu.rid from
        {schema}.point_counts_union as pcu, {schema}.tmp_load as tl
        where st_intersects(pcu.rast, tl.rast);

    -- Union overlapping rasters with new data, summing overlap values
    create table if not exists {schema}.tmp_union (rast raster);

    insert into {schema}.tmp_union
      select ST_Union(r.rast, 'SUM') as rast from
        (select rast from {schema}.point_counts_union
        where rid in (select rid from {schema}.overlaps)
        union all
        select rast from {schema}.tmp_load) as r;


    -- Delete overlapping rasters from main table
    delete from {schema}.point_counts_union
      where rid in (select rid from {schema}.overlaps);


    -- Insert updated data
    select DropRasterConstraints('{schema}'::name, 'point_counts_union'::name, 'rast'::name);

    insert into {schema}.point_counts_union (rast)
      select ST_Tile(rast, 100, 100) as raster
      from {schema}.tmp_union;

    select AddRasterConstraints('{schema}'::name, 'point_counts_union'::name, 'rast'::name);


    -- Delete records from temporary import tables
    delete from {schema}.tmp_load;
    delete from {schema}.tmp_union; ")


  message("Merging new and existing rasters")
  p <- dbsettings$POOL
  pool::dbExecute(p, cmd)
  pool::dbExecute(p, glue::glue("VACUUM ANALYZE {schema}.point_counts;"))
  pool::dbExecute(p, glue::glue("VACUUM ANALYZE {schema}.point_counts_union;"))
}


#' Import building points
#'
#' @export
#'
db_load_building_points <- function(dbsettings,
                                    las, metadata.id) {

  epsgcode <- lidR::epsg(las)
  schema <- .get_schema_for_epsg(dbsettings, epsgcode)

  p <- dbsettings$POOL

  dat <- CERMBlidar::get_building_points(las)
  if (nrow(dat) > 0) {
    values <- glue::glue_collapse(
      glue::glue("({metadata.id}, {dat$Z}, \\
                  ST_GeomFromText('{sf::st_as_text(dat$geometry)}', {epsgcode}))"),
      sep = ", "
    )

    command <- glue::glue("INSERT INTO {schema}.building_points \\
                            (meta_id, height, geom) VALUES {values};")

    pool::dbExecute(p, command)
  }
}


#' Query bounding polygons of imported LAS images
#'
#' This function queries the \code{'las_metadata'} table with optional
#' subsetting based on LAS image file names and/or capture dates. It returns an
#' \code{'sf'} spatial data frame containing the source file name, start and end
#' capture dates and times, and bounding rectangle of the point cloud as a
#' polygon. If no file name or date constraints are specified, all records in
#' the table are returned.
#'
#' @param dbsettings A named list of database connection settings returned
#'   by \code{db_connect_postgis} or \code{db_create_postgis}.
#'
#' @param file.pattern A string pattern to match LAS source file names. Matching is done ignoring case.
#'   The pattern can be any of the following:
#'   \itemize{
#'     \item{A plain string - to return file names containing this string;}
#'     \item{An SQL string expression with \code{'\%'} wildcard - e.g. \code{'Bega\%5946\%';}}
#'     \item{A regular expression - e.g. \code{'^Bega.+5946'}.}
#'   }

#'   The default (\code{NULL}) means match any file name.
#'
#' @param capture.start Beginning of time window for LAS images. Can be specified as a character string or a The default
#'   \code{NULL} means no start time constraint.
#'
#' @param capture.end End of time window for LAS images. The default \code{NULL}
#'   means no end time constraint.
#'
#' @return An \code{'sf'} spatial data frame containing the source file name,
#'   start and end capture dates and times, and bounding rectangle of the point
#'   cloud as a polygon.
#'
#' @export
#'
db_get_las_bounds <- function(dbsettings,
                              file.pattern = NULL,
                              capture.start = NULL, capture.end = NULL) {

  file.pattern <- .first_elem(file.pattern)
  capture.start <- .first_elem(capture.start)
  capture.end <- .first_elem(capture.end)

  where.cond <- NULL
  if (!is.null(file.pattern) | !is.null(capture.start) | !is.null(capture.end)) {
    if (!is.null(file.pattern)) {
      if (stringr::str_detect(file.pattern, "%"))
        where.cond <- c(where.cond, glue::glue("filename ILIKE '{file.pattern}'"))
      else if (stringr::str_detect(file.pattern, "[\\*\\+\\[\\]\\^\\$]+"))
        where.cond <- c(where.cond, glue::glue("filename ~* '{file.pattern}'"))
      else
        where.cond <- c(where.cond, glue::glue("filename ILIKE '%{file.pattern}%'"))
    }

    if (!is.null(capture.start))
      where.cond <- c(where.cond, glue::glue("capture_start >= '{.tformat(capture.start)}'"))

    if (!is.null(capture.end))
      where.cond <- c(where.cond, glue::glue("capture_end <= '{.tformat(capture.end)}'"))

    where.cond <- paste("WHERE", paste(where.cond, collapse=" AND "))

  } else {
    # No file name or time conditions
    where.cond <- ""
  }

  command <- glue::glue("SELECT filename, capture_start, capture_end,
                        ST_AsEWKT(bounds) AS geometry
                        FROM {dbsettings$TABLE_METADATA}
                        {where.cond};")

  p <- dbsettings$POOL
  res <- pool::dbGetQuery(p, command)

  if (nrow(res) > 0) {
    res$geometry <- sf::st_as_sfc(res$geometry)
  }
  else {
    # Empty result
    res <- data.frame(filename = character(0),
                      capture_start = as.POSIXct(character(0), tz = "UTC"),
                      capture_end = as.POSIXct(character(0), tz = "UTC"),
                      sf::st_sfc())
  }

  sf::st_sf(res)
}


#' Export raster point count data that intersects with one or more features
#'
#' This function takes a set of features (points, lines, polygons or an extent
#' in the form of a matrix or raster Extent object), identifies the raster tiles
#' of point count data that the features intersect with, and exports these to a
#' GeoTIFF file.
#'
#' @param dbsettings A named list of database connection settings returned
#'   by \code{db_connect_postgis} or \code{db_create_postgis}.
#'
#' @param geom The feature(s) to intersect. Can also be an \code{Extent} object
#'   or a 2x2 matrix where the first row is X min,max and the second row is
#'   Y min,max.
#'
#' @param outpath A path and filename for the output GeoTIFF file.
#'
#' @param default.epsg EPSG code to assume for the features if none has been
#'   assigned (e.g. with \code{sf::st_crs}).
#'
#' @return A raster stack linked to the exported GeoTIFF file.
#'
#' @seealso \code{\link{db_connect_postgis}} \code{\link{db_create_postgis}}
#'
#' @export
#'
db_export_point_counts <- function(dbsettings, geom, outpath,
                                default.epsg = 3308) {

  if (inherits(geom, what = c("sfc", "sfg")))
    g <- geom
  else if (inherits(geom, what = "sf")) {
    isgeom <- sapply(geom, inherits, what = "sfc")
    if (!sum(isgeom) == 1)
      stop("Failed to find geometry column in sf data frame")
    g <- geom[, isgeom, drop=TRUE]
  } else if (inherits(geom, what = c("matrix", "Extent"))) {
    g <- sf::st_sfc(.extentToPolygon(geom))
  }

  g.crs <- sf::st_crs(g)
  if (!is.na(g.crs)) epsg <- g.crs$epsg
  else epsg <- default.epsg

  g.values <- sapply(g, function(gg) {
    glue::glue("ST_GeomFromText('{sf::st_as_text(gg)}', {epsg})")
  })

  g.values <- paste( paste("(", g.values, ")"), collapse = ",")

  command <- glue::glue("
    CREATE TEMPORARY TABLE tmp_features (
      feature geometry(GEOMETRY, {epsg}) NOT NULL
    );
    --------
    INSERT INTO tmp_features (feature)
      VALUES {g.values};
    --------
    CREATE TEMPORARY TABLE tmp_export_rast AS
      SELECT lo_from_bytea(0, ST_AsGDALRaster(ST_Union(u.rast), 'GTiff')) AS loid
      FROM {dbsettings$TABLE_COUNTS_UNION} AS u, tmp_features as f
      WHERE ST_Intersects(u.rast, f.feature);
    --------
    SELECT lo_export(loid, '{outpath}')
      FROM tmp_export_rast;
    --------
    SELECT lo_unlink(loid)
      FROM tmp_export_rast;
    --------
    DROP TABLE tmp_features;
    DROP TABLE tmp_export_rast;
  ")

  p <- dbsettings$POOL
  pool::dbBegin(p)
  pool::dbExecute(p, command)
  pool::dbCommit(p)

  if (file.exists(outpath)) {
    r <- raster::stack(outpath)
    names(r) <- paste("height", 1:raster::nlayers(r), sep = ".")
    r
  } else {
    warning("Output GeoTIFF file was not created")
    NULL
  }
}


#' Check if a table exists in the given database
#'
#' @param dbsettings A named list of database connection settings returned
#'   by \code{db_connect_postgis} or \code{db_create_postgis}.
#'
#' @param tablename Name of the table in the form \code{schema.tablename}.
#'
#' @return \code{TRUE} if the table was found; \code{FALSE} otherwise.
#'
#' @seealso \code{\link{db_connect_postgis}} \code{\link{db_create_postgis}}
#'
#' @examples
#' \dontrun{
#' dbsettings <- db_connect_postgis(...)
#' pg_table_exists(dbsettings, "lidar.point_counts_union")
#' }
#'
#' @export
#'
pg_table_exists <- function(dbsettings, tablename) {
  p <- dbsettings$POOL
  command <- glue::glue("select count(*) as n from {tablename};")

  o <- options(warn = -1, show.error.messages = FALSE)
  x <- try(pool::dbGetQuery(p, command))
  options(o)

  !is.null(x)
}


# Retrieve the database schema that corresponds to an EPSG code
.get_schema_for_epsg <- function(dbsettings, epsg) {
  i <- match(epsg, dbsettings$epsg)
  if (is.na(i)) stop("No database schema supports EPSG code ", epsg)

  dbsettings$schema[i]
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


# Format a time stamp to include the time zone
# (used by db_load_tile_metadata)
.tformat <- function(timestamp, tz = "UTC") {
  if (inherits(timestamp, "POSIXt")){
    format(timestamp, usetz = TRUE)
  } else if (inherits(timestamp, "character")) {
    x <- lubridate::parse_date_time(timestamp,
                                    orders = c("ymd", "ymd H", "ymd HM", "ymd HMS"),
                                    tz=tz)
    format(timestamp, usetz = TRUE)
  } else {
    stop("Argument timestamp should be POSIXt or character")
  }
}


.first_elem <- function(x, msg = TRUE) {
  if (is.null(x)) {
    NULL
  }
  else if (length(x) > 1) {
    if (msg) {
      nm <- deparse(substitute(x))
      message("Ignoring all but first element for ", nm)
    }
    x[1]
  } else {
    x
  }
}


.extentToPolygon <- function(x) {
  stopifnot(inherits(x, c("matrix", "Extent")))

  if (class(x) == "matrix") {
    xy <- c(x[1,], x[2,])
  } else {
    xy <- c(x@xmin, x@xmax, x@ymin, x@ymax)
  }

  ii <- c(1,3, 1,4, 2,4, 2,3, 1,3)

  pts <- matrix(xy[ii], ncol=2, byrow = TRUE)
  sf::st_polygon(list(pts))
}

