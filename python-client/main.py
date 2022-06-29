from tech.mlsql.byzer_client_sdk import Byzer

# if __name__ == '__main__':
#     script = Byzer(). \
#         load(). \
#         format("csv"). \
#         path("/tmp/obc").options().add("header", "true").end(). \
#         end().to_script()
#     print(script)


import plotly.express as px

df = px.data.gapminder().query("country=='Canada'")

fig = px.line(df, x="year", y="lifeExp", title='Life expectancy in Canada')


