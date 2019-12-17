## NSW 100k map sheet numbers, names and boundaries

nswmaps100k <- sf::st_read("data-raw/nswmaps_shapefile/100k_250k.shp")

nswmaps100k <- nswmaps100k[, c("mapnumber", "maptitle", "geometry")]
colnames(nswmaps100k)[2] <- "mapname"

usethis::use_data(nswmaps100k, overwrite = TRUE)
