import { pool } from './postgres';

async function seed() {
  console.log('🌱 Seeding database...');

  await pool.query(`
    DROP TABLE IF EXISTS order_items CASCADE;
    DROP TABLE IF EXISTS orders CASCADE;
    DROP TABLE IF EXISTS products CASCADE;
    DROP TABLE IF EXISTS users CASCADE;
  `);

  await pool.query(`
    CREATE TABLE users (
      id SERIAL PRIMARY KEY,
      email VARCHAR(255) NOT NULL,
      name VARCHAR(255) NOT NULL,
      created_at TIMESTAMP DEFAULT NOW()
    );

    CREATE TABLE products (
      id SERIAL PRIMARY KEY,
      name VARCHAR(255) NOT NULL,
      category VARCHAR(100) NOT NULL,
      price DECIMAL(10,2) NOT NULL,
      stock INT NOT NULL DEFAULT 0
    );

    CREATE TABLE orders (
      id SERIAL PRIMARY KEY,
      user_id INT NOT NULL REFERENCES users(id),
      status VARCHAR(50) NOT NULL DEFAULT 'pending',
      total DECIMAL(10,2) NOT NULL,
      created_at TIMESTAMP DEFAULT NOW()
    );

    CREATE TABLE order_items (
      id SERIAL PRIMARY KEY,
      order_id INT NOT NULL REFERENCES orders(id),
      product_id INT NOT NULL REFERENCES products(id),
      quantity INT NOT NULL,
      unit_price DECIMAL(10,2) NOT NULL
    );
  `);

  // 10,000 users in batches of 100
  console.log('  Inserting users...');
  for (let i = 0; i < 100; i++) {
    const values = Array.from({ length: 100 }, (_, j) => {
      const idx = i * 100 + j + 1;
      return `('user${idx}@example.com', 'User ${idx}')`;
    }).join(',');
    await pool.query(`INSERT INTO users (email, name) VALUES ${values}`);
  }

  // 1,000 products
  console.log('  Inserting products...');
  const categories = ['Electronics', 'Clothing', 'Books', 'Home', 'Sports'];
  for (let i = 0; i < 10; i++) {
    const values = Array.from({ length: 100 }, (_, j) => {
      const idx = i * 100 + j + 1;
      const cat = categories[idx % categories.length];
      const price = (Math.random() * 500 + 10).toFixed(2);
      return `('Product ${idx}', '${cat}', ${price}, ${Math.floor(Math.random() * 1000)})`;
    }).join(',');
    await pool.query(`INSERT INTO products (name, category, price, stock) VALUES ${values}`);
  }

  // 50,000 orders
  console.log('  Inserting orders...');
  const statuses = ['pending', 'processing', 'shipped', 'delivered', 'cancelled'];
  for (let i = 0; i < 500; i++) {
    const values = Array.from({ length: 100 }, () => {
      const userId = Math.floor(Math.random() * 10000) + 1;
      const status = statuses[Math.floor(Math.random() * statuses.length)];
      const total = (Math.random() * 1000 + 10).toFixed(2);
      return `(${userId}, '${status}', ${total})`;
    }).join(',');
    await pool.query(`INSERT INTO orders (user_id, status, total) VALUES ${values}`);
  }

  // ~50,000 order items
  console.log('  Inserting order items...');
  for (let i = 0; i < 500; i++) {
    const values = Array.from({ length: 100 }, () => {
      const orderId = Math.floor(Math.random() * 50000) + 1;
      const productId = Math.floor(Math.random() * 1000) + 1;
      const qty = Math.floor(Math.random() * 5) + 1;
      const price = (Math.random() * 500 + 10).toFixed(2);
      return `(${orderId}, ${productId}, ${qty}, ${price})`;
    }).join(',');
    await pool.query(`INSERT INTO order_items (order_id, product_id, quantity, unit_price) VALUES ${values}`);
  }

  console.log('✅ Seeding complete.');
  await pool.end();
}

seed().catch(console.error);