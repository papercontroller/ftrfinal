-- PostgreSQL Schema for Endterm Resilient University Information System (UIS)
-- Student: Maulkan Sanzhar (CSE-2501M)
-- AITU Fault Tolerance and Dependable Computing

CREATE TABLE IF NOT EXISTS students (
    id SERIAL PRIMARY KEY,
    name VARCHAR(100) NOT NULL,
    email VARCHAR(100) UNIQUE NOT NULL,
    major VARCHAR(50) NOT NULL DEFAULT 'Computer Science',
    status VARCHAR(20) NOT NULL DEFAULT 'ACTIVE',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS transcripts (
    student_id INT PRIMARY KEY,
    gpa NUMERIC(3, 2) NOT NULL DEFAULT 3.80,
    credits INT NOT NULL DEFAULT 120,
    details JSONB NOT NULL DEFAULT '{"courses_completed": ["Algorithms", "Fault Tolerance", "Operating Systems"]}',
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (student_id) REFERENCES students(id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS payments (
    id SERIAL PRIMARY KEY,
    student_id INT NOT NULL,
    amount NUMERIC(10, 2) NOT NULL,
    currency VARCHAR(10) NOT NULL DEFAULT 'KZT',
    status VARCHAR(20) NOT NULL DEFAULT 'COMPLETED',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (student_id) REFERENCES students(id)
);

CREATE TABLE IF NOT EXISTS idempotency_keys (
    key VARCHAR(255) PRIMARY KEY,
    result TEXT NOT NULL,
    status_code INT NOT NULL DEFAULT 200,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS courses (
    id VARCHAR(20) PRIMARY KEY,
    name VARCHAR(100) NOT NULL,
    credits INT NOT NULL,
    max_capacity INT NOT NULL DEFAULT 30,
    enrolled INT NOT NULL DEFAULT 0
);

CREATE TABLE IF NOT EXISTS enrollments (
    id SERIAL PRIMARY KEY,
    student_id INT NOT NULL,
    course_id VARCHAR(20) NOT NULL,
    enrolled_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(student_id, course_id),
    FOREIGN KEY (student_id) REFERENCES students(id),
    FOREIGN KEY (course_id) REFERENCES courses(id)
);

-- Seed Initial Synthetic Data
INSERT INTO students (id, name, email, major, status) VALUES 
(1, 'Synthetic Student One', 'student1@example.test', 'Computer Science', 'ACTIVE'),
(2, 'Synthetic Student Two', 'student2@example.test', 'Software Engineering', 'ACTIVE'),
(3, 'Synthetic Student Three', 'student3@example.test', 'Cybersecurity', 'ACTIVE')
ON CONFLICT (id) DO NOTHING;

INSERT INTO transcripts (student_id, gpa, credits, details) VALUES 
(1, 3.85, 128, '{"courses_completed": ["Data Structures", "Fault Tolerance", "Distributed Systems"], "honors": "Dean List"}'),
(2, 3.70, 115, '{"courses_completed": ["Software Testing", "Cloud Architecture"], "honors": "None"}'),
(3, 3.92, 130, '{"courses_completed": ["Cryptography", "Network Security"], "honors": "Dean List"}')
ON CONFLICT (student_id) DO NOTHING;

INSERT INTO courses (id, name, credits, max_capacity, enrolled) VALUES 
('CSE2501', 'Fault Tolerance & Dependable Computing', 5, 30, 24),
('CSE2502', 'Cloud Microservices & Resilience', 5, 30, 28),
('CSE2503', 'Software Verification & Security', 5, 30, 19)
ON CONFLICT (id) DO NOTHING;
