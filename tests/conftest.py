"""
tests/conftest.py — shared fixtures for all test modules.
"""
from __future__ import annotations

from pathlib import Path
from typing import Any, Optional
from unittest.mock import MagicMock

import pytest
from sqlalchemy import (
    Column, Date, Float, ForeignKey, Integer, String, Text,
    create_engine,
)
from sqlalchemy.orm import declarative_base

Base = declarative_base()


# ── Legacy mem_engine schema (unchanged) ─────────────────────────────────────

class Customer(Base):
    __tablename__ = "customers"
    id     = Column(Integer, primary_key=True)
    name   = Column(String)
    tier   = Column(String)    # 'basic' | 'pro' | 'enterprise'  → category
    notes  = Column(Text)      # will hold JSON strings


class Order(Base):
    __tablename__ = "orders"
    id          = Column(Integer, primary_key=True)
    customer_id = Column(Integer, ForeignKey("customers.id"))
    amount      = Column(Integer)
    status      = Column(String)


@pytest.fixture(scope="session")
def mem_engine():
    """In-memory SQLite with populated dummy data (shared across all tests)."""
    engine = create_engine("sqlite:///:memory:")
    Base.metadata.create_all(engine)

    customers = [
        {"id": i, "name": f"User{i}",
         "tier": "enterprise" if i % 20 == 0 else ("pro" if i % 5 == 0 else "basic"),
         "notes": '{"theme":"dark","lang":"en"}'}
        for i in range(1, 101)
    ]
    orders = [
        {"id": i, "customer_id": (i % 100) + 1, "amount": i * 10, "status": "paid"}
        for i in range(1, 201)
    ]

    with engine.connect() as conn:
        conn.execute(Customer.__table__.insert(), customers)
        conn.execute(Order.__table__.insert(), orders)
        conn.commit()

    return engine


# ── Rich engine schema (6 tables) ────────────────────────────────────────────

RichBase = declarative_base()


class RichUser(RichBase):
    __tablename__ = "users"
    id         = Column(Integer, primary_key=True)
    name       = Column(String(100))
    email      = Column(String(150))
    country    = Column(String(50))
    city       = Column(String(100))
    status     = Column(String(20))   # active | inactive
    age        = Column(Integer)
    joined_at  = Column(String(20))   # ISO date string


class RichProduct(RichBase):
    __tablename__ = "products"
    id           = Column(Integer, primary_key=True)
    name         = Column(String(150))
    category     = Column(String(80))
    price        = Column(Float)
    cost_price   = Column(Float)
    stock        = Column(Integer)
    supplier     = Column(String(100))
    rating       = Column(Float)


class RichOrder(RichBase):
    __tablename__ = "orders"
    id             = Column(Integer, primary_key=True)
    user_id        = Column(Integer, ForeignKey("users.id"))
    order_date     = Column(String(20))
    total_amount   = Column(Float)
    status         = Column(String(20))   # completed | pending | cancelled
    discount       = Column(Float)
    payment_method = Column(String(30))   # card | cash | online


class RichOrderItem(RichBase):
    __tablename__ = "order_items"
    id         = Column(Integer, primary_key=True)
    order_id   = Column(Integer, ForeignKey("orders.id"))
    product_id = Column(Integer, ForeignKey("products.id"))
    quantity   = Column(Integer)
    unit_price = Column(Float)


class RichReview(RichBase):
    __tablename__ = "reviews"
    id          = Column(Integer, primary_key=True)
    user_id     = Column(Integer, ForeignKey("users.id"))
    product_id  = Column(Integer, ForeignKey("products.id"))
    rating      = Column(Integer)
    comment     = Column(Text)
    reviewed_at = Column(String(20))


class RichEmployee(RichBase):
    __tablename__ = "employees"
    id         = Column(Integer, primary_key=True)
    name       = Column(String(100))
    department = Column(String(80))
    salary     = Column(Float)
    hire_date  = Column(String(20))
    manager_id = Column(Integer, ForeignKey("employees.id"), nullable=True)


@pytest.fixture(scope="session")
def rich_engine():
    """
    Session-scoped in-memory SQLite engine with 6 richly populated tables:
    users, products, orders, order_items, reviews, employees.
    """
    engine = create_engine("sqlite:///:memory:")
    RichBase.metadata.create_all(engine)

    # ── Users (20) ────────────────────────────────────────────────────────────
    users = [
        {"id": 1,  "name": "Alice Johnson",  "email": "alice@example.com",   "country": "US", "city": "New York",    "status": "active",   "age": 30, "joined_at": "2022-03-15"},
        {"id": 2,  "name": "Bob Smith",      "email": "bob@example.com",     "country": "UK", "city": "London",      "status": "active",   "age": 45, "joined_at": "2022-06-22"},
        {"id": 3,  "name": "Carol White",    "email": "carol@example.com",   "country": "IN", "city": "Mumbai",      "status": "inactive", "age": 28, "joined_at": "2022-09-10"},
        {"id": 4,  "name": "David Brown",    "email": "david@example.com",   "country": "US", "city": "Chicago",     "status": "active",   "age": 52, "joined_at": "2022-11-01"},
        {"id": 5,  "name": "Eva Martinez",   "email": "eva@example.com",     "country": "US", "city": "Houston",     "status": "active",   "age": 35, "joined_at": "2023-01-18"},
        {"id": 6,  "name": "Frank Lee",      "email": "frank@example.com",   "country": "UK", "city": "Manchester",  "status": "inactive", "age": 41, "joined_at": "2023-02-28"},
        {"id": 7,  "name": "Grace Kim",      "email": "grace@example.com",   "country": "IN", "city": "Delhi",       "status": "active",   "age": 26, "joined_at": "2023-04-05"},
        {"id": 8,  "name": "Henry Wilson",   "email": "henry@example.com",   "country": "US", "city": "Phoenix",     "status": "active",   "age": 38, "joined_at": "2023-05-12"},
        {"id": 9,  "name": "Irene Chen",     "email": "irene@example.com",   "country": "UK", "city": "Birmingham",  "status": "active",   "age": 33, "joined_at": "2023-06-20"},
        {"id": 10, "name": "Jack Patel",     "email": "jack@example.com",    "country": "IN", "city": "Bangalore",   "status": "inactive", "age": 29, "joined_at": "2023-07-07"},
        {"id": 11, "name": "Karen Davis",    "email": "karen@example.com",   "country": "US", "city": "San Antonio", "status": "active",   "age": 47, "joined_at": "2023-08-14"},
        {"id": 12, "name": "Leo Garcia",     "email": "leo@example.com",     "country": "US", "city": "Dallas",      "status": "active",   "age": 31, "joined_at": "2023-09-03"},
        {"id": 13, "name": "Mia Thompson",   "email": "mia@example.com",     "country": "UK", "city": "Leeds",       "status": "inactive", "age": 25, "joined_at": "2023-10-22"},
        {"id": 14, "name": "Nate Robinson",  "email": "nate@example.com",    "country": "IN", "city": "Chennai",     "status": "active",   "age": 54, "joined_at": "2023-11-30"},
        {"id": 15, "name": "Olivia Hall",    "email": "olivia@example.com",  "country": "US", "city": "San Jose",    "status": "active",   "age": 37, "joined_at": "2024-01-08"},
        {"id": 16, "name": "Paul Young",     "email": "paul@example.com",    "country": "UK", "city": "Glasgow",     "status": "active",   "age": 42, "joined_at": "2024-02-14"},
        {"id": 17, "name": "Quinn Allen",    "email": "quinn@example.com",   "country": "IN", "city": "Hyderabad",   "status": "inactive", "age": 27, "joined_at": "2024-03-19"},
        {"id": 18, "name": "Rachel Scott",   "email": "rachel@example.com",  "country": "US", "city": "Austin",      "status": "active",   "age": 50, "joined_at": "2024-04-25"},
        {"id": 19, "name": "Sam Turner",     "email": "sam@example.com",     "country": "UK", "city": "Liverpool",   "status": "active",   "age": 34, "joined_at": "2024-06-01"},
        {"id": 20, "name": "Tina Adams",     "email": "tina@example.com",    "country": "IN", "city": "Pune",        "status": "active",   "age": 44, "joined_at": "2024-07-15"},
    ]

    # ── Products (15) ─────────────────────────────────────────────────────────
    raw_products = [
        {"id": 1,  "name": "Laptop Pro",        "category": "Electronics", "price": 1499.99, "stock": 50,  "supplier": "TechCorp",    "rating": 4.5},
        {"id": 2,  "name": "Wireless Mouse",     "category": "Electronics", "price": 29.99,  "stock": 200, "supplier": "Peripherals", "rating": 4.2},
        {"id": 3,  "name": "USB-C Hub",          "category": "Electronics", "price": 49.99,  "stock": 150, "supplier": "Peripherals", "rating": 4.0},
        {"id": 4,  "name": "Noise Headphones",   "category": "Electronics", "price": 299.99, "stock": 80,  "supplier": "AudioTech",   "rating": 4.7},
        {"id": 5,  "name": "4K Monitor",         "category": "Electronics", "price": 799.99, "stock": 30,  "supplier": "DisplayCo",   "rating": 4.6},
        {"id": 6,  "name": "Cotton T-Shirt",     "category": "Clothing",    "price": 19.99,  "stock": 500, "supplier": "FashionHub",  "rating": 3.9},
        {"id": 7,  "name": "Denim Jeans",        "category": "Clothing",    "price": 59.99,  "stock": 300, "supplier": "FashionHub",  "rating": 4.1},
        {"id": 8,  "name": "Winter Jacket",      "category": "Clothing",    "price": 149.99, "stock": 120, "supplier": "WarmWear",    "rating": 4.4},
        {"id": 9,  "name": "Running Shoes",      "category": "Clothing",    "price": 89.99,  "stock": 180, "supplier": "SportStyle",  "rating": 4.3},
        {"id": 10, "name": "Coffee Maker",       "category": "Home",        "price": 79.99,  "stock": 90,  "supplier": "HomeApply",   "rating": 4.2},
        {"id": 11, "name": "Blender Pro",        "category": "Home",        "price": 129.99, "stock": 60,  "supplier": "HomeApply",   "rating": 4.0},
        {"id": 12, "name": "Desk Lamp",          "category": "Home",        "price": 34.99,  "stock": 250, "supplier": "LightCo",     "rating": 3.8},
        {"id": 13, "name": "Organic Coffee",     "category": "Food",        "price": 14.99,  "stock": 400, "supplier": "BeanSource",  "rating": 4.6},
        {"id": 14, "name": "Protein Powder",     "category": "Food",        "price": 44.99,  "stock": 220, "supplier": "NutriLab",    "rating": 4.1},
        {"id": 15, "name": "Mixed Nuts Pack",    "category": "Food",        "price": 9.99,   "stock": 600, "supplier": "SnackCo",     "rating": 3.7},
    ]
    products = [
        {**p, "cost_price": round(p["price"] * 0.60, 2)}
        for p in raw_products
    ]

    # ── Orders (30) ───────────────────────────────────────────────────────────
    orders = [
        {"id": 1,  "user_id": 1,  "order_date": "2023-01-05", "total_amount": 1529.98, "status": "completed", "discount": 0.0,  "payment_method": "card"},
        {"id": 2,  "user_id": 2,  "order_date": "2023-02-14", "total_amount": 89.97,   "status": "completed", "discount": 5.0,  "payment_method": "online"},
        {"id": 3,  "user_id": 3,  "order_date": "2023-03-20", "total_amount": 149.99,  "status": "cancelled", "discount": 0.0,  "payment_method": "card"},
        {"id": 4,  "user_id": 4,  "order_date": "2023-04-08", "total_amount": 299.99,  "status": "completed", "discount": 10.0, "payment_method": "cash"},
        {"id": 5,  "user_id": 5,  "order_date": "2023-05-15", "total_amount": 829.98,  "status": "completed", "discount": 0.0,  "payment_method": "card"},
        {"id": 6,  "user_id": 6,  "order_date": "2023-05-22", "total_amount": 49.99,   "status": "pending",   "discount": 0.0,  "payment_method": "online"},
        {"id": 7,  "user_id": 7,  "order_date": "2023-06-10", "total_amount": 214.98,  "status": "completed", "discount": 15.0, "payment_method": "card"},
        {"id": 8,  "user_id": 8,  "order_date": "2023-06-28", "total_amount": 79.99,   "status": "completed", "discount": 0.0,  "payment_method": "cash"},
        {"id": 9,  "user_id": 9,  "order_date": "2023-07-03", "total_amount": 1799.98, "status": "completed", "discount": 0.0,  "payment_method": "card"},
        {"id": 10, "user_id": 10, "order_date": "2023-07-19", "total_amount": 19.99,   "status": "cancelled", "discount": 0.0,  "payment_method": "online"},
        {"id": 11, "user_id": 11, "order_date": "2023-08-04", "total_amount": 174.97,  "status": "completed", "discount": 5.0,  "payment_method": "card"},
        {"id": 12, "user_id": 12, "order_date": "2023-08-21", "total_amount": 259.98,  "status": "pending",   "discount": 0.0,  "payment_method": "online"},
        {"id": 13, "user_id": 13, "order_date": "2023-09-07", "total_amount": 44.99,   "status": "completed", "discount": 0.0,  "payment_method": "cash"},
        {"id": 14, "user_id": 14, "order_date": "2023-09-15", "total_amount": 529.97,  "status": "completed", "discount": 20.0, "payment_method": "card"},
        {"id": 15, "user_id": 15, "order_date": "2023-10-02", "total_amount": 89.98,   "status": "completed", "discount": 0.0,  "payment_method": "online"},
        {"id": 16, "user_id": 16, "order_date": "2023-10-18", "total_amount": 349.98,  "status": "cancelled", "discount": 0.0,  "payment_method": "card"},
        {"id": 17, "user_id": 17, "order_date": "2023-11-01", "total_amount": 129.99,  "status": "pending",   "discount": 10.0, "payment_method": "cash"},
        {"id": 18, "user_id": 18, "order_date": "2023-11-25", "total_amount": 1499.99, "status": "completed", "discount": 0.0,  "payment_method": "card"},
        {"id": 19, "user_id": 19, "order_date": "2023-12-08", "total_amount": 69.98,   "status": "completed", "discount": 5.0,  "payment_method": "online"},
        {"id": 20, "user_id": 20, "order_date": "2023-12-30", "total_amount": 94.98,   "status": "completed", "discount": 0.0,  "payment_method": "card"},
        {"id": 21, "user_id": 1,  "order_date": "2024-01-12", "total_amount": 329.98,  "status": "completed", "discount": 0.0,  "payment_method": "online"},
        {"id": 22, "user_id": 3,  "order_date": "2024-02-05", "total_amount": 59.99,   "status": "pending",   "discount": 0.0,  "payment_method": "card"},
        {"id": 23, "user_id": 5,  "order_date": "2024-02-20", "total_amount": 449.98,  "status": "completed", "discount": 15.0, "payment_method": "cash"},
        {"id": 24, "user_id": 7,  "order_date": "2024-03-01", "total_amount": 34.99,   "status": "completed", "discount": 0.0,  "payment_method": "online"},
        {"id": 25, "user_id": 9,  "order_date": "2024-03-18", "total_amount": 799.99,  "status": "completed", "discount": 0.0,  "payment_method": "card"},
        {"id": 26, "user_id": 11, "order_date": "2024-04-04", "total_amount": 124.97,  "status": "cancelled", "discount": 0.0,  "payment_method": "online"},
        {"id": 27, "user_id": 13, "order_date": "2024-04-22", "total_amount": 269.98,  "status": "completed", "discount": 5.0,  "payment_method": "card"},
        {"id": 28, "user_id": 15, "order_date": "2024-05-10", "total_amount": 199.98,  "status": "pending",   "discount": 0.0,  "payment_method": "cash"},
        {"id": 29, "user_id": 17, "order_date": "2024-06-03", "total_amount": 879.98,  "status": "completed", "discount": 10.0, "payment_method": "card"},
        {"id": 30, "user_id": 19, "order_date": "2024-07-01", "total_amount": 59.98,   "status": "completed", "discount": 0.0,  "payment_method": "online"},
    ]

    # ── Order Items (45) ──────────────────────────────────────────────────────
    order_items = [
        {"id": 1,  "order_id": 1,  "product_id": 1,  "quantity": 1, "unit_price": 1499.99},
        {"id": 2,  "order_id": 1,  "product_id": 2,  "quantity": 1, "unit_price": 29.99},
        {"id": 3,  "order_id": 2,  "product_id": 6,  "quantity": 2, "unit_price": 19.99},
        {"id": 4,  "order_id": 2,  "product_id": 15, "quantity": 5, "unit_price": 9.99},
        {"id": 5,  "order_id": 3,  "product_id": 8,  "quantity": 1, "unit_price": 149.99},
        {"id": 6,  "order_id": 4,  "product_id": 4,  "quantity": 1, "unit_price": 299.99},
        {"id": 7,  "order_id": 5,  "product_id": 5,  "quantity": 1, "unit_price": 799.99},
        {"id": 8,  "order_id": 5,  "product_id": 2,  "quantity": 1, "unit_price": 29.99},
        {"id": 9,  "order_id": 6,  "product_id": 3,  "quantity": 1, "unit_price": 49.99},
        {"id": 10, "order_id": 7,  "product_id": 7,  "quantity": 2, "unit_price": 59.99},
        {"id": 11, "order_id": 7,  "product_id": 12, "quantity": 3, "unit_price": 34.99},
        {"id": 12, "order_id": 8,  "product_id": 10, "quantity": 1, "unit_price": 79.99},
        {"id": 13, "order_id": 9,  "product_id": 5,  "quantity": 2, "unit_price": 799.99},
        {"id": 14, "order_id": 10, "product_id": 6,  "quantity": 1, "unit_price": 19.99},
        {"id": 15, "order_id": 11, "product_id": 9,  "quantity": 1, "unit_price": 89.99},
        {"id": 16, "order_id": 11, "product_id": 13, "quantity": 3, "unit_price": 14.99},
        {"id": 17, "order_id": 11, "product_id": 15, "quantity": 2, "unit_price": 9.99},
        {"id": 18, "order_id": 12, "product_id": 4,  "quantity": 1, "unit_price": 299.99},
        {"id": 19, "order_id": 13, "product_id": 14, "quantity": 1, "unit_price": 44.99},
        {"id": 20, "order_id": 14, "product_id": 1,  "quantity": 1, "unit_price": 1499.99},
        {"id": 21, "order_id": 14, "product_id": 6,  "quantity": 2, "unit_price": 19.99},
        {"id": 22, "order_id": 15, "product_id": 9,  "quantity": 1, "unit_price": 89.99},
        {"id": 23, "order_id": 16, "product_id": 8,  "quantity": 1, "unit_price": 149.99},
        {"id": 24, "order_id": 16, "product_id": 11, "quantity": 1, "unit_price": 129.99},
        {"id": 25, "order_id": 16, "product_id": 15, "quantity": 7, "unit_price": 9.99},
        {"id": 26, "order_id": 17, "product_id": 11, "quantity": 1, "unit_price": 129.99},
        {"id": 27, "order_id": 18, "product_id": 1,  "quantity": 1, "unit_price": 1499.99},
        {"id": 28, "order_id": 19, "product_id": 6,  "quantity": 2, "unit_price": 19.99},
        {"id": 29, "order_id": 19, "product_id": 15, "quantity": 3, "unit_price": 9.99},
        {"id": 30, "order_id": 20, "product_id": 9,  "quantity": 1, "unit_price": 89.99},
        {"id": 31, "order_id": 21, "product_id": 4,  "quantity": 1, "unit_price": 299.99},
        {"id": 32, "order_id": 21, "product_id": 2,  "quantity": 1, "unit_price": 29.99},
        {"id": 33, "order_id": 22, "product_id": 7,  "quantity": 1, "unit_price": 59.99},
        {"id": 34, "order_id": 23, "product_id": 5,  "quantity": 1, "unit_price": 799.99},
        {"id": 35, "order_id": 23, "product_id": 3,  "quantity": 3, "unit_price": 49.99},
        {"id": 36, "order_id": 24, "product_id": 12, "quantity": 1, "unit_price": 34.99},
        {"id": 37, "order_id": 25, "product_id": 5,  "quantity": 1, "unit_price": 799.99},
        {"id": 38, "order_id": 26, "product_id": 13, "quantity": 5, "unit_price": 14.99},
        {"id": 39, "order_id": 26, "product_id": 14, "quantity": 2, "unit_price": 44.99},
        {"id": 40, "order_id": 27, "product_id": 8,  "quantity": 1, "unit_price": 149.99},
        {"id": 41, "order_id": 27, "product_id": 10, "quantity": 1, "unit_price": 79.99},
        {"id": 42, "order_id": 28, "product_id": 11, "quantity": 1, "unit_price": 129.99},
        {"id": 43, "order_id": 29, "product_id": 5,  "quantity": 1, "unit_price": 799.99},
        {"id": 44, "order_id": 29, "product_id": 9,  "quantity": 1, "unit_price": 89.99},
        {"id": 45, "order_id": 30, "product_id": 6,  "quantity": 3, "unit_price": 19.99},
    ]

    # ── Reviews (25) ──────────────────────────────────────────────────────────
    reviews = [
        {"id": 1,  "user_id": 1,  "product_id": 1,  "rating": 5, "comment": "Excellent laptop!",       "reviewed_at": "2023-02-01"},
        {"id": 2,  "user_id": 2,  "product_id": 6,  "rating": 4, "comment": "Good quality shirt",       "reviewed_at": "2023-03-05"},
        {"id": 3,  "user_id": 3,  "product_id": 8,  "rating": 3, "comment": "Decent jacket",            "reviewed_at": "2023-04-12"},
        {"id": 4,  "user_id": 4,  "product_id": 4,  "rating": 5, "comment": "Amazing headphones",       "reviewed_at": "2023-05-08"},
        {"id": 5,  "user_id": 5,  "product_id": 5,  "rating": 5, "comment": "Best monitor ever",        "reviewed_at": "2023-06-15"},
        {"id": 6,  "user_id": 6,  "product_id": 3,  "rating": 4, "comment": "Works great",              "reviewed_at": "2023-06-30"},
        {"id": 7,  "user_id": 7,  "product_id": 7,  "rating": 4, "comment": "Nice jeans",               "reviewed_at": "2023-07-20"},
        {"id": 8,  "user_id": 8,  "product_id": 10, "rating": 4, "comment": "Makes great coffee",       "reviewed_at": "2023-08-10"},
        {"id": 9,  "user_id": 9,  "product_id": 5,  "rating": 5, "comment": "Stunning display",         "reviewed_at": "2023-08-22"},
        {"id": 10, "user_id": 10, "product_id": 6,  "rating": 2, "comment": "Fabric faded quickly",     "reviewed_at": "2023-09-04"},
        {"id": 11, "user_id": 11, "product_id": 9,  "rating": 4, "comment": "Very comfortable",         "reviewed_at": "2023-09-18"},
        {"id": 12, "user_id": 12, "product_id": 4,  "rating": 5, "comment": "Noise cancellation rocks", "reviewed_at": "2023-10-02"},
        {"id": 13, "user_id": 13, "product_id": 14, "rating": 3, "comment": "Average taste",            "reviewed_at": "2023-10-15"},
        {"id": 14, "user_id": 14, "product_id": 1,  "rating": 5, "comment": "Worth every penny",        "reviewed_at": "2023-10-28"},
        {"id": 15, "user_id": 15, "product_id": 9,  "rating": 4, "comment": "Great for running",        "reviewed_at": "2023-11-05"},
        {"id": 16, "user_id": 16, "product_id": 8,  "rating": 3, "comment": "Ok for the price",         "reviewed_at": "2023-11-20"},
        {"id": 17, "user_id": 17, "product_id": 11, "rating": 4, "comment": "Smooth blending",          "reviewed_at": "2023-12-03"},
        {"id": 18, "user_id": 18, "product_id": 1,  "rating": 5, "comment": "Perfect for work",         "reviewed_at": "2023-12-18"},
        {"id": 19, "user_id": 19, "product_id": 6,  "rating": 4, "comment": "Comfortable fit",          "reviewed_at": "2024-01-07"},
        {"id": 20, "user_id": 20, "product_id": 9,  "rating": 5, "comment": "Excellent support",        "reviewed_at": "2024-01-22"},
        {"id": 21, "user_id": 1,  "product_id": 2,  "rating": 4, "comment": "Precise clicks",           "reviewed_at": "2024-02-08"},
        {"id": 22, "user_id": 3,  "product_id": 7,  "rating": 3, "comment": "Normal jeans",             "reviewed_at": "2024-03-01"},
        {"id": 23, "user_id": 5,  "product_id": 3,  "rating": 5, "comment": "Very handy hub",           "reviewed_at": "2024-03-20"},
        {"id": 24, "user_id": 7,  "product_id": 12, "rating": 1, "comment": "Stopped working fast",     "reviewed_at": "2024-04-10"},
        {"id": 25, "user_id": 9,  "product_id": 5,  "rating": 5, "comment": "Incredible picture",       "reviewed_at": "2024-05-05"},
    ]

    # ── Employees (12) ────────────────────────────────────────────────────────
    employees = [
        {"id": 1,  "name": "James Director",  "department": "Engineering", "salary": 120000.0, "hire_date": "2019-03-01", "manager_id": None},
        {"id": 2,  "name": "Anna Lead",       "department": "Engineering", "salary": 95000.0,  "hire_date": "2020-06-15", "manager_id": 1},
        {"id": 3,  "name": "Chris Dev",       "department": "Engineering", "salary": 80000.0,  "hire_date": "2021-09-10", "manager_id": 2},
        {"id": 4,  "name": "Diana Dev",       "department": "Engineering", "salary": 78000.0,  "hire_date": "2022-01-20", "manager_id": 2},
        {"id": 5,  "name": "Ethan Head",      "department": "Marketing",   "salary": 110000.0, "hire_date": "2018-11-05", "manager_id": None},
        {"id": 6,  "name": "Fiona Mgr",       "department": "Marketing",   "salary": 88000.0,  "hire_date": "2020-04-22", "manager_id": 5},
        {"id": 7,  "name": "George Analyst",  "department": "Marketing",   "salary": 65000.0,  "hire_date": "2022-07-18", "manager_id": 6},
        {"id": 8,  "name": "Hannah VP",       "department": "Sales",       "salary": 115000.0, "hire_date": "2019-08-12", "manager_id": None},
        {"id": 9,  "name": "Ivan Rep",        "department": "Sales",       "salary": 72000.0,  "hire_date": "2021-02-28", "manager_id": 8},
        {"id": 10, "name": "Julia Rep",       "department": "Sales",       "salary": 68000.0,  "hire_date": "2022-11-09", "manager_id": 8},
        {"id": 11, "name": "Kevin HR Head",   "department": "HR",          "salary": 90000.0,  "hire_date": "2020-05-30", "manager_id": None},
        {"id": 12, "name": "Laura HR Spec",   "department": "HR",          "salary": 58000.0,  "hire_date": "2023-03-14", "manager_id": 11},
    ]

    with engine.connect() as conn:
        conn.execute(RichUser.__table__.insert(), users)
        conn.execute(RichProduct.__table__.insert(), products)
        conn.execute(RichOrder.__table__.insert(), orders)
        conn.execute(RichOrderItem.__table__.insert(), order_items)
        conn.execute(RichReview.__table__.insert(), reviews)
        conn.execute(RichEmployee.__table__.insert(), employees)
        conn.commit()

    return engine


# ── nd_fixture helper ─────────────────────────────────────────────────────────

def nd_fixture(tmp_path: Path, engine, stub_sql: str = "SELECT 1") -> Any:
    """
    Build a NexusData-like namespace with:
    - Real KBManager and ConversationGraph pointed at tmp_path
    - Stubbed LLMController (no real API calls)
    - Real SQLite engine from rich_engine
    - All pipeline components wired up

    Returns a simple namespace object with .ask() and other key methods
    stubbed/wired for integration testing.
    """
    from nexus_data.kb.manager import KBManager
    from nexus_data.kb.vector_repo import VectorQueryRepo
    from nexus_data.kb.conversation_graph import ConversationGraph
    from nexus_data.kb.graph_store import SQLGraphStore
    from nexus_data.kb.entity_tracker import EntityTracker
    from nexus_data.kb.bookmarks import BookmarkStore
    from nexus_data.pipeline.normalizer import QueryNormalizer
    from nexus_data.pipeline.goal_identifier import GoalIdentifierAgent
    from nexus_data.pipeline.reference_resolver import ReferenceResolverAgent
    from nexus_data.pipeline.decomposer import QueryDecomposer
    from nexus_data.pipeline.planner import PlannerAgent
    from nexus_data.pipeline.executor import ExecutorAgent
    from nexus_data.models import QueryResult

    kb_dir = tmp_path / "kb"
    kb_dir.mkdir(parents=True, exist_ok=True)

    kb = KBManager(kb_dir=kb_dir)

    # Write a minimal db_info so schema names are discoverable
    db_info_content = (
        "# Database Topology\n\n"
        "## Table: `users`\n"
        "- `id` (INTEGER)\n- `name` (TEXT)\n- `email` (TEXT)\n"
        "- `country` (TEXT)\n- `city` (TEXT)\n- `status` (TEXT)\n"
        "- `age` (INTEGER)\n- `joined_at` (TEXT)\n\n"
        "## Table: `products`\n"
        "- `id` (INTEGER)\n- `name` (TEXT)\n- `category` (TEXT)\n"
        "- `price` (REAL)\n- `cost_price` (REAL)\n- `stock` (INTEGER)\n"
        "- `supplier` (TEXT)\n- `rating` (REAL)\n\n"
        "## Table: `orders`\n"
        "- `id` (INTEGER)\n- `user_id` (INTEGER)\n- `order_date` (TEXT)\n"
        "- `total_amount` (REAL)\n- `status` (TEXT)\n- `discount` (REAL)\n"
        "- `payment_method` (TEXT)\n\n"
        "## Table: `order_items`\n"
        "- `id` (INTEGER)\n- `order_id` (INTEGER)\n- `product_id` (INTEGER)\n"
        "- `quantity` (INTEGER)\n- `unit_price` (REAL)\n\n"
        "## Table: `reviews`\n"
        "- `id` (INTEGER)\n- `user_id` (INTEGER)\n- `product_id` (INTEGER)\n"
        "- `rating` (INTEGER)\n- `comment` (TEXT)\n- `reviewed_at` (TEXT)\n\n"
        "## Table: `employees`\n"
        "- `id` (INTEGER)\n- `name` (TEXT)\n- `department` (TEXT)\n"
        "- `salary` (REAL)\n- `hire_date` (TEXT)\n- `manager_id` (INTEGER)\n"
    )
    kb.write_db_info(db_info_content)

    # Stubbed LLM — returns stub_sql for generate(), "data" for classify
    stub_llm = MagicMock()
    stub_llm.generate.return_value = (
        '{"operation": "select", "time_frame": "none", "filters": [], '
        '"grouping": [], "metrics": [], "ordering": "none", "limit": null, '
        '"relevant_tables": ["users"], "intent_summary": "show users", '
        '"ambiguous": false, "clarification_question": null, "skip_cache": false, '
        '"needs_window_function": false, "needs_subquery": false, '
        '"is_percentage_or_ratio": false}'
    )
    stub_llm.generate_sql_fix.return_value = stub_sql
    stub_llm.summarise_result.return_value = "Here are the results."
    stub_llm.explain_sql.return_value = "This query selects all users."

    vector_repo = MagicMock()
    vector_repo.search_canonical_sql.return_value = None
    vector_repo.save_canonical_sql.return_value = None
    vector_repo.record_hit.return_value = None
    vector_repo.record_correction.return_value = None

    graph_store = SQLGraphStore(kb_dir=kb_dir)
    entity_tracker = EntityTracker()
    bookmarks = BookmarkStore(kb_dir=kb_dir)
    conv_graph = ConversationGraph(kb_dir=kb_dir)

    normalizer = QueryNormalizer(kb, vector_repo)
    identifier = GoalIdentifierAgent(stub_llm, kb)
    resolver = ReferenceResolverAgent(kb, conv_graph)
    decomposer = QueryDecomposer(stub_llm, kb)
    planner = PlannerAgent(stub_llm, kb, dialect="sqlite", graph_store=graph_store)
    executor = ExecutorAgent(engine, stub_llm, kb, vector_repo)

    # Wire into a lightweight namespace object
    class _ND:
        def __init__(self):
            self._kb = kb
            self._llm = stub_llm
            self._vector_repo = vector_repo
            self._graph_store = graph_store
            self._entity_tracker = entity_tracker
            self._bookmarks = bookmarks
            self._conv_graph = conv_graph
            self._normalizer = normalizer
            self._identifier = identifier
            self._resolver = resolver
            self._decomposer = decomposer
            self._planner = planner
            self._executor = executor
            self._engine = engine
            self._result_history: dict = {}

        def ask(self, nl_query: str) -> QueryResult:
            from nexus_data.critic.guardian import Guardian, PromptInjectionError
            _guard = Guardian()
            try:
                nl_query = _guard.check_user_input(nl_query)
            except PromptInjectionError as exc:
                return QueryResult(sql="", error=str(exc))

            if len(nl_query) > 4000:
                return QueryResult(
                    sql="",
                    error=f"Query too long ({len(nl_query)} chars, max 4000). Please shorten your question.",
                )

            norm = self._normalizer.normalize(nl_query)
            goal = self._identifier.identify(norm)
            resolved = self._resolver.resolve(goal)

            if self._entity_tracker.has_pronoun(nl_query):
                entity_ctx = self._entity_tracker.resolve_context()
                if entity_ctx:
                    resolved.resolved_goal_json["_entity_context"] = entity_ctx

            decomp = self._decomposer.decompose(nl_query, resolved.resolved_goal_json)
            if decomp.is_complex and decomp.enriched_goal.get("_decomposition"):
                resolved.resolved_goal_json["_decomposition"] = decomp.enriched_goal["_decomposition"]

            plan = self._planner.plan(resolved)
            result = self._executor.execute(plan)

            if not result.error and not result.is_clarification and result.rows:
                self._entity_tracker.ingest_result(result.columns, result.rows)
                result.natural_language_summary = self._llm.summarise_result(
                    result.sql, result.rows, result.columns
                )
            if not goal.is_ambiguous:
                self._entity_tracker.ingest_filters(goal.goal_dict.get("filters", []))

            return result

        def save_bookmark(self, name: str) -> str:
            last = self._kb.get_last_turn_record()
            if not last:
                return "Nothing to bookmark."
            self._bookmarks.save(name, last.get("user_query", ""), last.get("sql", ""))
            return f"Bookmarked as '{name}'."

        def run_bookmark(self, name: str) -> QueryResult:
            entry = self._bookmarks.get(name)
            if not entry:
                return QueryResult(sql="", error=f"Bookmark '{name}' not found.")
            from nexus_data.critic.guardian import Guardian, SafetyViolation
            from nexus_data.critic.self_healer import execute_with_healing
            guardian = Guardian()
            try:
                res = execute_with_healing(
                    engine=self._engine,
                    sql=entry[1],
                    llm_fix_fn=lambda bad, err: entry[1],
                    guardian_validate_fn=guardian.validate,
                )
                res.from_cache = True
                return res
            except SafetyViolation as exc:
                return QueryResult(sql=entry[1], error=f"Safety Check Failed: {exc}")

    return _ND()
