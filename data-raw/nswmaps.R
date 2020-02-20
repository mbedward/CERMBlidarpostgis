## NSW 100k map sheet numbers, names and boundaries

nswmaps100k <- sf::st_read("data-raw/nswmaps_shapefile/100k_250k.shp")

nswmaps100k <- nswmaps100k[, c("mapnumber", "maptitle", "geometry")]
colnames(nswmaps100k)[2] <- "mapname"

# CRS is defined but we need to set the integer EPSG code
sf::st_crs(nswmaps100k) <- 4326

usethis::use_data(nswmaps100k, overwrite = TRUE)
