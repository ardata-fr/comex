library(curl)
library(zip)
library(here)
library(data.table)

# CSV files ----
url_export_2019 <- "https://www.data.gouv.fr/fr/datasets/r/0a78bacc-99cf-4a26-8f28-088f2e96a2ef"
url_import_2019 <- "https://www.data.gouv.fr/fr/datasets/r/ded883a3-8bfd-4344-bb59-a7767815c494"

dir.create(here("data"), showWarnings = FALSE, recursive = TRUE)

curl_download(
  url = url_export_2019,
  destfile = here("data/202002_regional_2019_export.zip"))
curl_download(
  url = url_import_2019,
  destfile = here("data/202002_regional_2019_import.zip"))

unzip(zipfile = here("data/202002_regional_2019_export.zip"),
      exdir = here("data"))
unzip(zipfile = here("data/202002_regional_2019_import.zip"),
      exdir = here("data"))


# read files -------
label_A129 <- fread(
  file = here("data/Regional-2019-export/Libelle_A129.txt"),
  col.names = c("code_A129", "label_A129"),
  header = TRUE,
  colClasses = list(character = c(1, 2)),
  select = 1:2,
  encoding = "UTF-8"
)
setDF(label_A129)

label_CPF4 <- fread(
  file = here("data/Regional-2019-export/Libelle_CPF4.txt"),
  col.names = c("code_CPF4", "label_CPF4"),
  header = TRUE,
  colClasses = list(character = c(1, 2)),
  select = 1:2,
  encoding = "UTF-8"
)
setDF(label_CPF4)

label_PAYS <- fread(
  file = here("data/Regional-2019-export/Libelle_PAYS.txt"),
  col.names = c("code_pays", "label_pays"),
  header = TRUE,
  na.strings = "",
  colClasses = list(character = c(1, 2)),
  select = 1:2,
  encoding = "UTF-8"
)
setDF(label_PAYS)

export_2019 <- fread(
  file = here("data/Regional-2019-export/Region_2019_Export.txt"),
  na.strings = "",
  header = FALSE,
  colClasses = list(character = c(1, 4, 5, 6, 7, 8)),
  col.names = c("flux", "trimestre", "annee", "code_dep",
                "code_reg", "code_A129", "code_CPF4",
                "code_pays", "valeur_exp", "masse_exp")
)
export_2019[, flux:=NULL]
setDF(export_2019)

import_2019 <- fread(
  file = here("data/Regional-2019-import/Region_2019_Import.txt"),
  colClasses = list(character = c(1, 4, 5, 6, 7, 8)),
  na.strings = "",
  header = FALSE,
  col.names = c("flux", "trimestre", "annee", "code_dep",
                "code_reg", "code_A129", "code_CPF4",
                "code_pays", "valeur_imp", "masse_imp")
)
import_2019[, flux:=NULL]
setDF(import_2019)


# write parquet ----
arrow::write_parquet(export_2019, here("data/export_2019.parquet"))
arrow::write_parquet(import_2019, here("data/import_2019.parquet"))
arrow::write_parquet(label_A129,  here("data/label_A129.parquet"))
arrow::write_parquet(label_CPF4,  here("data/label_CPF4.parquet"))
arrow::write_parquet(label_PAYS,  here("data/label_PAYS.parquet"))

unlink(c(here("data/202002_regional_2019_export.zip"),
         here("data/202002_regional_2019_import.zip"),
         here("data/Regional-2019-export"),
         here("data/Regional-2019-import")
         ),
       recursive = TRUE,
       force = TRUE)
