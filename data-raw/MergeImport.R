## code to prepare `MergeImport` dataset goes here

SQL_MergeImportRaster <- glue::glue("
-- Merge a raster in temporary table tmp_load into
-- two tables:
-- 'point_counts' which holds rasters as imported;
-- 'point_counts_union' which holds mosaiced rasters where pixel
-- values in overlap areas are the sum of input values for LAS rasters.

-- Copy data into point_counts table
insert into rasters.point_counts (rast)
select rast from rasters.tmp_load;

-- Identify existing rasters that overlap the new data
create or replace view rasters.overlaps as
select pcu.rid from
rasters.point_counts_union as pcu, rasters.tmp_load as tl
where st_intersects(pcu.rast, tl.rast);

-- Union overlapping rasters with new data, summing overlap values
create table if not exists rasters.tmp_union (rast raster);

insert into rasters.tmp_union
select ST_Union(r.rast, 'SUM') as rast from
(select rast from rasters.point_counts_union
   where rid in (select rid from rasters.overlaps)
 union all
 select rast from tmp_load) as r;


-- Delete overlapping rasters from main table
delete from rasters.point_counts_union
where rid in (select rid from rasters.overlaps);


-- Insert updated data
select DropRasterConstraints('rasters'::name, 'point_counts_union'::name, 'rast'::name);

insert into rasters.point_counts_union (rast)
select ST_Tile(rast, 100, 100) as raster
from rasters.tmp_union;

select AddRasterConstraints('rasters'::name, 'point_counts_union'::name, 'rast'::name);


-- Delete records from temporary import tables
delete from tmp_load;
delete from tmp_union;
")

usethis::use_data(SQL_MergeImportRaster, overwrite = TRUE)
