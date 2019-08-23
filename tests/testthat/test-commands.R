context("Test parsing of command strings for psql.exe")

test_that("Single line commands are parsed", {
  fn <- CERMBlidarpostgis:::.parse_command

  cmd <- "select * from foo;"
  regex <- "select \\* from foo;"

  expect_match(fn(cmd), regex)

  # leading / trailing space
  expect_match(fn(paste("   ", cmd, "   ")), regex)
})


test_that("Multi-line commands are parsed", {
  fn <- CERMBlidarpostgis:::.parse_command

  target <- "select \\* from foo where answer == 42;"

  out <- fn(c("select * from", "foo where", "answer == 42;"))
  expect_match(out, target)

  out <- fn(c("  ", "", "select * from", "foo where", "answer == 42;", "  "))
  expect_match(out, target)
})


test_that("Empty commands are set to NULL", {
  fn <- CERMBlidarpostgis:::.parse_command

  expect_null(fn(""))
  expect_null(fn("   "))
  expect_null(fn(c("  ", "     ", " ")))
  expect_null(fn(NULL))
  expect_null(fn(character(0)))
})


test_that("Special psql.exe commands are parsed", {
  fn <- CERMBlidarpostgis:::.parse_command

  expect_match(fn("\\dt"), "\\\\dt")
  expect_match(fn("\\d mytable"), "\\\\d mytable")
})
