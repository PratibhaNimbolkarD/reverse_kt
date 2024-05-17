CREATE DATABASE CompanyDB;

USE CompanyDB;

CREATE TABLE Employees (
    EmployeeID INT PRIMARY KEY,
    FirstName VARCHAR(50),
    LastName VARCHAR(50),
    Department VARCHAR(50),
    Salary INT,
    HireDate DATE
);

INSERT INTO Employees (EmployeeID, FirstName, LastName, Department, Salary, HireDate)
VALUES
(1, 'Pratibha', 'Nimbolkar', 'HR', 60000, '2020-01-15'),
(2, 'Himani', 'Garg', 'IT', 75000, '2019-03-22'),
(3, 'Vipul', 'Tapare', 'Finance', 80000, '2021-07-19'),
(4, 'Anjali', 'Saxena', 'IT', 72000, '2018-05-11'),
(5, 'Neha', 'Kumari', 'Marketing', 65000, '2022-02-25'),
(6, 'Naina', 'Saraswat', 'Finance', 75000, '2021-02-25');

SELECT * FROM Employees
WHERE Department = 'IT';

SELECT * FROM Employees
ORDER BY Salary DESC;

SELECT Department, AVG(Salary) AS AverageSalary
FROM Employees
GROUP BY Department;

SELECT Department, MIN(Salary) AS MinSalary 
FROM Employees 
GROUP BY Department;

SELECT Department, AVG(Salary) AS AverageSalary
FROM Employees
GROUP BY Department
HAVING AVG(Salary) > 65000;

SELECT SUM(Salary) AS TotalSalary
FROM Employees;

SELECT COUNT(DISTINCT Department) AS NumberOfDepartments
FROM Employees;

