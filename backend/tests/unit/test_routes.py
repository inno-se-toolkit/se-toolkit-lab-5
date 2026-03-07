def test_analytics_routes_exist():
    from app.main import app
    routes = [route.path for route in app.routes]
    print("Available routes:", routes)
    assert "/analytics/scores" in routes
