##########
# Kerncijfers wijken and buurten (xls_flow)

# Taking 2013-2020
# Taking 2013 here, because earlier data has different format, so we leave integration of those for later. #TODO
# https://www.cbs.nl/nl-nl/reeksen/kerncijfers-wijken-en-buurten-2004-2020
kwb_years = range(2013, 2021)
kwb_urls = [
    f"https://www.cbs.nl/-/media/cbs/dossiers/nederland-regionaal/wijk-en-buurtstatistieken/_exel/kwb-{year}.xls"
    for year in kwb_years
]

##########
# Nabijheidsstatistieken


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

# Bevolkingsstatistieken per pc4
"83502NED"

##########
# Mapping pc6huisnummer tot buurten and wijken
