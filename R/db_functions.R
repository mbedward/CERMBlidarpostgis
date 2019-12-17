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
#' @param las.paths The paths and filenames of the LAS source files. This can
#'   be an uncompressed (\code{.las}) or compressed \code{.laz} or \code{.zip}
#'   file.
#'
#' @param dem.paths The paths and file names of DEM (digital elevation model)
#'   rasters corresponding to the LAS source files. If a vector of paths is
#'   provided it must be the same length as \code{las.paths}. For each element
#'   that specifies the path to a raster file (e.g. GeoTIFF) or zipped raster
#'   file, the DEM data will read and used to normalize the point heights for
#'   the corresponding LAS file. Where an element is \code{NA} or
#'   an empty string, point heights for the corresponding LAS file will be
#'   normalized using Delaunay triangulation. If this argument is set to
#'   \code{NULL} (default), triangulation will be used for all LAS files.
#'
#' @param mapnames Specifies the mapsheet names to store for the imported
#'   LAS tiles. If \code{NULL} (default), mapsheet names are extracted from the
#'   leading alphabetic portion of the LAS file names. Alternatively it can be
#'   a character vector of either length 1 (same name for all LAS tiles) or
#'   length equal to that of \code{las.paths}.
#'
#' @param check.overlap If \code{TRUE} (default), images will be checked for
#'   overlap between flight lines using the function
#'   \code{remove_flightline_overlap} in the \code{CERMBlidar} package. If an
#'   image fails the overlap check (i.e. it appears to have overlap and this
#'   cannot be removed) it will be skipped. Setting this argument to
#'   \code{FALSE} disables overlap checking. This is useful for images with
#'   unusual configurations of flight lines that cause a false positive overlap
#'   check.
#'
#' @param union.rasters If \code{TRUE}, raster point counts derived from the
#'   LAS images will be loaded into both the \code{point_counts} table as a
#'   single record and the \code{point_counts_union} table where data for
#'   spatially adjacent, overlapping rasters are merged. If \code{FALSE}
#'   (default), raster point counts are only loaded into the \code{point_counts}
#'   table.
#'
#' @param union.batch If \code{union.rasters = TRUE}, this defines how many
#'   rasters to load before merging data into the \code{point_counts_union}
#'   table. The default, and minimum allowable, value is 1.
#'
#' @export
#'
db_import_las <- function(dbsettings,
                          las.paths,
                          dem.paths = NULL,
                          mapnames = NULL,
                          check.overlap = TRUE,
                          union.rasters = FALSE,
                          union.batch = 1) {

  p <- .get_pool(dbsettings)

  if (union.rasters) {
    warning("Beware!!!",
            "Merging of rasters is presently experimental and will probably",
            "end in tears", immediate. = TRUE)

    if (union.batch < 1) stop("The argument union.batch must be >= 1")
  }

  fn_check_exists <- function(paths, label) {
    x <- sapply(paths, file.exists)
    if (any(!x)) {
      x <- x[!x]
      if (length(x) <= 5) {
        msg <- paste("The following", label, "files cannot be found:")
        msg <- paste(msg, paste(names(x), collapse = "\n"), sep = "\n")
      } else {
        msg <- paste(length(x), label, "files cannot be found including:")
        msg <- paste(msg, paste(head(names(x), 5), collapse = "\n"), sep = "\n")
      }
      stop(msg)
    }
  }

  fn_check_exists(las.paths, "LAS")

  if (!is.null(dem.paths)) {
    if (length(las.paths) != length(dem.paths)) {
      stop("When dem.paths is not NULL it must be the same length as las.paths")
    }

    non.empty.paths <- stringr::str_subset(dem.paths, "[^\\s]")
    fn_check_exists(non.empty.paths, "DEM")

    # Set any empty paths to NA
    ii <- stringr::str_length(stringr::str_trim(dem.paths)) == 0
    dem.paths[ii] <- NA
  }


  imported <- rep(0, length(las.paths))
  n.union <- 0
  Nlas <- length(las.paths)

  for (i in 1:Nlas) {
    las.file <- las.paths[i]

    if (db_lasfile_imported(dbsettings, las.file)) {
      message("Skipping previously imported file: ", .file_from_path(las.file))
    } else {
      msg <- glue::glue("{i}/{Nlas} {.file_from_path(las.file)}")
      message(msg)

      dem.file <- dem.paths[i]
      if (!is.null(dem.file)) {
        if (is.na(dem.file)) dem.file <- NULL
      }

      if (is.null(mapnames)) {
        mapname <- stringr::str_extract(.file_from_path(las.file), "^[A-Za-z]+")
      } else if (length(mapnames) == 1) {
        mapname <- mapnames
      } else {
        mapname <- mapnames[i]
      }

      tryCatch({
        .do_import_las(dbsettings, las.file, dem.file, mapname, check.overlap, batch.mode)
        imported[i] <- 1

        if (union.rasters) {
          n.union <- n.union + 1
          if (n.union >= union.batch) {
            .do_merge_new_rasters(dbsettings, schema = NULL)
            n.union <- 0
          }
        }
      },
      error = function(e) {
        imported[i] <- -1
      })
    }
  }

  # If union-ing, process any remaining rasters
  if (union.rasters) {
    .do_merge_new_rasters(dbsettings, schema = NULL)
  }

  imported
}


# Non-exported helper function for db_import_las
#
.do_import_las <- function(dbsettings,
                          las.path,
                          dem.path = NULL,
                          mapname,
                          check.overlap,
                          batch.mode) {

  message("Reading data and normalizing point heights")

  if (is.null(dem.path))
    las <- CERMBlidar::prepare_tile(las.path, normalize.heights = "tin")
  else
    las <- CERMBlidar::prepare_tile(las.path, normalize.heights = dem.path)

  if (check.overlap) {
    message("Removing any flight line overlap")
    las <- CERMBlidar::remove_flightline_overlap(las)
  } else {
    message("Skipping flight line overlap check")
  }

  message("Importing LAS metadata")
  metadata.id <- db_load_tile_metadata(dbsettings,
                                       las, las.path, mapname)

  message("Importing point counts for strata")
  db_load_stratum_counts(dbsettings,
                         las = las,
                         metadata.id = metadata.id,
                         batch.mode = batch.mode)

  message("Importing building points")
  db_load_building_points(dbsettings,
                          las = las,
                          metadata.id = metadata.id)
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
  p <- .get_pool(dbsettings)
  found <- logical(length(filenames))

  fname <- .file_remove_extension( .file_from_path(filenames) )

  # Search all schemas
  for (schema in dbsettings$schema) {
    tblname <- glue::glue("{schema}.{dbsettings$TABLE_METADATA}")
    command <- glue::glue("SELECT filename FROM {tblname};")
    x <- pool::dbGetQuery(p, command)$filename
    if (length(x) > 0) {
      found <- found | sapply(fname, function(f) tolower(f) %in% tolower(x))
    }
  }

  found
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
#' @return The integer \code{rid} value of the record(s) for the imported
#'   raster.
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

  p <- .get_pool(dbsettings)

  is.tbl <- pg_table_exists(dbsettings, tablename)

  if (replace) {
    in.mode <- "-d"
  } else if (is.tbl) {
    in.mode <- "-a"
  } else {
    in.mode <- "-c"
  }

  if (!replace && is.tbl) {
    # Get current rid values for records in this table
    cmd <- glue::glue("select rid from {tablename};")
    existing.recs <- pool::dbGetQuery(p, cmd)
  } else {
    existing.recs <- data.frame(rid = integer(0))
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

  # Return rid values for newly inserted raster recs
  cmd <- glue::glue("select rid from {tablename};")
  all.recs <- pool::dbGetQuery(p, cmd)

  setdiff(all.recs$rid, existing.recs$rid)
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
#' @param mapname Name of the map sheet (assumed to be 100k topographic map) to
#'   assign to this LAS tile.
#'
#' @return The integer value of the \code{'id'} field for the newly created
#'   database record.
#'
#' @seealso \code{\link{db_connect_postgis}} \code{\link{db_create_postgis}}
#'
#' @export
#'
db_load_tile_metadata <- function(dbsettings,
                                  las, filename, mapname) {

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
    (filename, mapname, capture_start, capture_end, \\
    area, \\
    point_density, \\
    npts_ground, npts_veg, npts_building, npts_water, npts_other, npts_total, \\
    nflightlines,
    bounds)
    values(\\
    '{filename}', \\
    '{mapname}', \\
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

  p <- .get_pool(dbsettings)
  pool::dbExecute(p, command)

  # Return id value of the record just created
  res <- pool::dbGetQuery(p, glue::glue("select id from {tblname} \\
                                        where filename = '{filename}'"))

  res$id[1]
}


#' Rasterize and import points counts
#'
#' @param dbsettings A named list of database connection settings returned
#'   by \code{db_connect_postgis} or \code{db_create_postgis}.
#'
#' @param las A LAS object.
#'
#' @param metadata.id Integer ID value of the corresponding record in the
#'   \code{'las_metadata'} table.
#'
#' @param batch.mode If \code{TRUE}, merging of raster data into the
#'   \code{'point_counts_union'} table is deferred to speed up the import of
#'   a group of LAS files. If \code{FALSE} (default), merging is performed.
#'
#' @export
#'
db_load_stratum_counts <- function(dbsettings, las, metadata.id, batch.mode = FALSE) {

  counts <- CERMBlidar::get_stratum_counts(las, CERMBlidar::StrataCERMB)

  schema <- .get_schema_for_epsg(dbsettings, lidR::epsg(las))

  # Load point counts for this tile into the temp table. If the table does
  # not exist it will be created, otherwise the new data will be appended
  # to the existing table.
  new.rids <- pg_load_raster(dbsettings,
                             counts,
                             epsg = lidR::epsg(las),
                             tablename = glue::glue("{schema}.tmp_load"),
                             replace = FALSE,
                             tilew = ncol(counts),
                             tileh = nrow(counts))

  # Copy the new data into the point_counts table
  new.rids.clause <- paste(new.rids, sep = ", ")
  cmd <- glue::glue("
      insert into {schema}.point_counts (meta_id, rast)
        select {metadata.id} as meta_id, rast from {schema}.tmp_load
        where rid in ({new.rids.clause});")

  p <- .get_pool(dbsettings)
  pool::dbExecute(p, cmd)

  if (!batch.mode) {
    .do_merge_new_rasters(dbsettings, schema)
  }
}


# Non-exported function to merge newly imported rasters in the tmp_load
# table into point_counts_union
#
.do_merge_new_rasters <- function(dbsettings, schema = NULL) {
  if (is.null(schema)) {
    # check all schemas
    schemas <- dbsettings$schema
  } else {
    schemas <- schema
  }

  for (schema in schemas) {
    tmp_load <- glue::glue("{schema}.tmp_load")
    tmp_union <- glue::glue("{schema}.tmp_union")

    if (pg_table_exists(dbsettings, tmp_load)) {
      p <- .get_pool(dbsettings)

      # Check that there is some newly imported data
      cmd <- glue::glue("select count(rast) as N from {tmp_load};")
      x <- pool::dbGetQuery(p, cmd)

      if (x$N > 0) {
        # Merge the new and existing rasters
        cmd <- glue::glue("
        -- Identify existing rasters that overlap the new data
        create or replace view {schema}.overlaps as
          select pcu.rid from
            {schema}.point_counts_union as pcu, {tmp_load} as tl
            where st_intersects(pcu.rast, tl.rast);

        -- Union overlapping rasters with new data, summing overlap values
        create table if not exists {tmp_union} (rast raster);

        -- In case the tmp_union table already existed, clear all rows
        truncate table {tmp_union};

        insert into {tmp_union}
          select ST_Union(r.rast, 'SUM') as rast from
            (select rast from {schema}.point_counts_union
            where rid in (select rid from {schema}.overlaps)
            union all
            select rast from {tmp_load}) as r;

        -- Delete overlapping rasters from main table
        delete from {schema}.point_counts_union
          where rid in (select rid from {schema}.overlaps);

        -- Insert updated data
        select DropRasterConstraints('{schema}'::name, 'point_counts_union'::name, 'rast'::name);

        insert into {schema}.point_counts_union (rast)
          select ST_Tile(rast, 100, 100) as raster
          from {tmp_union};

        select AddRasterConstraints('{schema}'::name, 'point_counts_union'::name, 'rast'::name);

        -- Delete records from the temporary tables
        truncate table {tmp_load};
        truncate table {tmp_union}; ")

        msg <- glue::glue("Merging new and existing rasters in {schema}")
        message(msg)

        p <- .get_pool(dbsettings)
        pool::dbExecute(p, cmd)
        pool::dbExecute(p, glue::glue("VACUUM ANALYZE {schema}.point_counts;"))
        pool::dbExecute(p, glue::glue("VACUUM ANALYZE {schema}.point_counts_union;"))
      }
    }
  }
}


#' Import building points
#'
#' @export
#'
db_load_building_points <- function(dbsettings,
                                    las, metadata.id) {

  epsgcode <- lidR::epsg(las)
  schema <- .get_schema_for_epsg(dbsettings, epsgcode)

  p <- .get_pool(dbsettings)

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

  p <- .get_pool(dbsettings)
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

  p <- .get_pool(dbsettings)
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


#' Summarize number of LAS tiles by map name
#'
#' Gets a summary of the number of LAS tiles stored in the database by
#' map zone (schema), map name and year.
#'
#' @param dbsettings A named list of database connection settings returned
#'   by \code{db_connect_postgis} or \code{db_create_postgis}.
#'
#' @return A data frame giving, for each combination of map zone (schema),
#'   map name and year, the number of LAS tiles imported.
#'
#' @export
#'
db_summary <- function(dbsettings) {
  p <- .get_pool(dbsettings)

  # Search all schemas
  res <- lapply(dbsettings$schema, function(schema) {
    command <- glue::glue("select '{schema}' as zone, mapname, year, count(id) as ntiles from (
                          select substring(filename, '^[^\\d]+') as mapname,
                            extract (year from capture_start) as year,
                            id from {schema}.{dbsettings$TABLE_METADATA}) as foo
                          group by (mapname, year) order by (mapname, year);")

    pool::dbGetQuery(p, command)
  })

  dplyr::bind_rows(res)
}


#' Submit an arbitrary SQL query
#'
#' This is a short-cut function that retrieves the database connection object from a
#' \code{dbsettings} list, submits an SQL query, and returns the results as a data frame.
#' It is an alternative to composing queries as \code{dplyr} pipelines.
#'
#' @param dbsettings A named list of database connection settings returned
#'   by \code{db_connect_postgis} or \code{db_create_postgis}.
#'
#' @param query SQL query as a character string or similar (e.g. \code{glue}
#'   string).
#'
#' @export
#'
#' @examples
#' \dontrun{
#' Sys.setenv(PGPASSWORD = "secret")
#' DBSettings <- db_connect_postgis("cermb_lidar")
#'
#' # Simple query - count records in one of the tables
#' res <- db_get_query(DBSettings, "select count(*) as n from mgazone56.point_counts")
#'
#' # A more complex query using the glue package to compose the SQL string
#' library(glue)
#'
#' mapnames <- c("Dorrigo", "Drake", "Gosford")
#'
#' values <- glue_collapse(glue("'{mapnames}'"), sep = ", ")
#'
#' query <- glue::glue("select id, mapname, capture_start from
#'                        (select id, substring(filename, '^[^\\d]+') as mapname, capture_start
#'                         from mgazone56.las_metadata) as foo
#'                      where mapname in ({values});")
#'
#' res <- db_get_query(DBSettings, query)
#' }
#'
db_get_query <- function(dbsettings, query) {
  if (!stringr::str_detect(query, "\\;\\s*$")) query <- paste0(query, ";")
  p <- .get_pool(dbsettings)
  pool::dbGetQuery(p, query)
}


#' Check for any records in metadata tables with no associated point count rasters
#'
#' Each record in a \code{point_counts} table, which contains rasters of point
#' counts for vertical strata, is linked to a parent record in the
#' \code{las_metadata} table within the same schema (e.g. 'mgazone56') via the
#' relation \code{point_counts.meta_id = las_metadata.id}. The database will not
#' allow a point counts record without an associated metadata record, but it
#' will allow a metadata record without any point counts records. Usually, this
#' will indicate that some problem occurred during import. This function checks
#' for any such childless metadata records.
#'
#' @param dbsettings A named list of database connection settings returned
#'   by \code{db_connect_postgis} or \code{db_create_postgis}.
#'
#' @param schema A character vector giving the names of one or more schemas
#'   (e.g. "mgazone56") to check. If \code{NULL} (default), all schemas are
#'   checked.
#'
#' @return A data frame. If childless meta-data records were found, the schema,
#'   integer id and filename will be listed for each. If no such records were
#'   found the data frame will have zero rows and columns.
#'
#' @export
#'
db_check_childless_metadata <- function(dbsettings, schema = NULL) {
  if (is.null(schema)) schema <- dbsettings$schema

  res <- lapply(schema, function(s) {
    cmd <- glue::glue("select '{s}' as schema, id, filename from {s}.{tbl.meta} \\
                      where id NOT IN (
                        select meta_id from {s}.{tbl.pcounts})")

    db_get_query(dbsettings, cmd)
  })

  dplyr::bind_rows(res)
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
  p <- .get_pool(dbsettings)
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
.file_from_path <- function(paths) {
  stringr::str_extract(paths, "[\\w\\.\\-]+$")
}

# Remove extension from filename
.file_remove_extension <- function(filenames) {
  ext <- "\\.\\w+$"

  out <- sapply(filenames, function(fname) {
    if (stringr::str_detect(fname, ext)) {
      pos <- stringr::str_locate(fname, ext)[1,1] - 1
    } else {
      pos <- -1
    }
    stringr::str_sub(fname, 1, pos)
  })

  unname(out)
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

