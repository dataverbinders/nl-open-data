# Creates a `CBS helper` dataset in BQ, with 4 (/5?) tables:
# - CBS Catalog
# - Kerncijfers wijken and buurten
# - Nabijheidsstatistieken
# - Bevolkingsstatistieken per pc4
# - Mapping pc6huisnummer tot buurten and wijken

##########
# Upload Kerncijfers wijken and buurten to gcs (xls_concat_flow)

# Taking 2013-2020
# Taking 2013 here, because earlier data has different format, so we leave integration of those for later. #TODO
# https://www.cbs.nl/nl-nl/reeksen/kerncijfers-wijken-en-buurten-2004-2020
kwb_urls = [
    "https://www.cbs.nl/-/media/cbs/dossiers/nederland-regionaal/wijk-en-buurtstatistieken/_exel/kwb-2013.xls",
    "https://www.cbs.nl/-/media/cbs/dossiers/nederland-regionaal/wijk-en-buurtstatistieken/_exel/kerncijfers-wijken-en-buurten-2014.xls",
    "https://www.cbs.nl/-/media/cbs/dossiers/nederland-regionaal/wijk-en-buurtstatistieken/_exel/kwb-2015.xls",
    "https://www.cbs.nl/-/media/cbs/dossiers/nederland-regionaal/wijk-en-buurtstatistieken/_exel/kwb-2016.xls",
    "https://www.cbs.nl/-/media/cbs/dossiers/nederland-regionaal/wijk-en-buurtstatistieken/_exel/kwb-2017.xls",
    "https://www.cbs.nl/-/media/_excel/2021/12/kwb-2018.xls",
    "https://www.cbs.nl/-/media/_excel/2021/12/kwb-2019.xls",
    "https://www.cbs.nl/-/media/_excel/2021/12/kwb-2020.xls",
]

##########
# Upload Nabijheidsstatistieken to gcs (Xls_flow + statline_bq_flow)


# 2006-2016 figures are excel files
"https://www.cbs.nl/-/media/_excel/2016/17/nabijheid-2006-2016-04-18.xls"
"https://www.cbs.nl/-/media/_excel/2016/17/nabijheid-2007-2016-04-18.xls"
"https://www.cbs.nl/-/media/_excel/2016/17/nabijheid-2008-2016-04-18.xls"
"https://www.cbs.nl/-/media/_excel/2016/17/nabijheid-2009-2016-04-18.xls"
"https://www.cbs.nl/-/media/_excel/2016/17/nabijheid-2010-2016-04-18.xls"
"https://www.cbs.nl/-/media/_excel/2016/16/nabijheid-2011-2016-04-18.xls"
"https://www.cbs.nl/-/media/_excel/2016/16/nabijheid-2012-2016-04-18.xls"
"https://www.cbs.nl/-/media/_excel/2016/51/nabijheid-2013-2016-12-19.xls"
"https://www.cbs.nl/-/media/_excel/2016/51/nabijheid-2014-2016-10-11-(1).xls"
"https://www.cbs.nl/-/media/_excel/2017/32/nabijheid_wijkbuurt_2015v3.xls"
"https://www.cbs.nl/-/media/_excel/2017/32/nabijheid_2016.xls"
# 2017 onwards in datasets:
"84334NED"
"84463NED"
"84718NED"
###########

# Bevolkingsstatistieken per pc4 (statline_bq_flow)
"83502NED"

##########
# Create dataset (gcs_to_bq_flow)


##########
# Mapping pc6huisnummer tot buurten and wijken (????)
