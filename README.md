# Homework 2 — Student Records with MongoDB and Spark SQL

**Course:** Big Data (BSc Program)  
**Implementation:** Scala 2.13.18 · Apache Spark 4.2.0 · MongoDB Scala Driver 5.5.1  
**Runtime:** Java 17 · sbt · Windows 11 / IntelliJ IDEA

## Overview

This project implements a two-stage student-records data pipeline:

1. **MongoDB (operational queries):** parse the CSV, insert 100 student documents into `university.students`, create indexes, and search interactively by course, GPA, or both.
2. **Spark SQL (analytical queries):** load a JSON export of the same records, normalize the schema, and execute SQL queries involving arrays, aggregations, joins, and window functions. Save the per-course summary to Parquet and verify the saved data.

The project uses **Option B** of Part 4: Spark reads the exported JSON file instead of connecting directly to MongoDB.

## Project Structure

```text
HW2-BigData/
├── build.sbt
├── .gitignore
├── data/
│   └── hw2-data.csv
├── output/
│   ├── students.json
│   └── course_stats/          # Spark-generated Parquet directory
├── project/                   # sbt configuration
└── src/
    ├── main/
    │   └── scala/
    │       ├── Main.scala
    │       ├── Student.scala
    │       ├── StudentImporter.scala
    │       ├── MongoStudentRepository.scala
    │       ├── StudentJsonExporter.scala
    │       ├── MongoQueries.scala
    │       └── SparkAnalysis.scala
    └── test/
```

IDE and build directories such as `.idea/`, `.bsp/`, and `target/` are not needed to understand or run the source code.

## Prerequisites

- **JDK 17** (tested with Eclipse Temurin 17.0.20)
- **Scala 2.13.18** (configured through sbt)
- **Apache Spark SQL 4.2.0** (resolved through sbt)
- **MongoDB**, running locally at `mongodb://localhost:27017`
- **sbt** and optionally **IntelliJ IDEA** with Scala support

The `build.sbt` declares the Spark SQL and MongoDB Scala Driver dependencies. No Python installation is needed for this Scala implementation.

## Data Model

The input file, `data/hw2-data.csv`, contains 100 records with the columns:

`ID`, `FirstName`, `LastName`, `DateOfBirth`, `CourseIDs`, `GPA`.

The `CourseIDs` field contains an unquoted list such as `[101,102,103]`. The importer handles the embedded commas rather than using a naive comma split.

A MongoDB student document has the following logical structure:

```javascript
{
  _id: 1,
  firstName: "Alice",
  lastName: "Johnson",
  dateOfBirth: ISODate("1998-03-15T00:00:00Z"),
  courseIds: [101, 102, 103],
  gpa: 3.8
}
```

MongoDB stores `dateOfBirth` as a date. The JSON export represents dates as `yyyy-MM-dd` strings; Spark converts these to its `date` type when loading.

## How to Run

Start the local MongoDB server first. Open the project in IntelliJ IDEA, import it as an sbt project, and use the **sbt shell** (or run the Scala main classes directly).

From the sbt shell:

```text
compile
runMain Main
runMain MongoQueries
runMain SparkAnalysis
```

**Recommended order:**

1. `runMain Main` — import student data into MongoDB, create indexes, and export JSON. The import is designed to be re-runnable; do not run it unnecessarily if you want to preserve the current collection.
2. `runMain MongoQueries` — run interactive MongoDB queries. The menu supports:
   - `1` — search by Course ID
   - `2` — search by minimum GPA (strictly greater than)
   - `3` — search by both Course ID and minimum GPA
   - `4` — exit
3. `runMain SparkAnalysis` — execute Parts 4.1–4.4 and save/read the Parquet output.

Run commands from the **project root**, because the code uses relative paths such as `data/hw2-data.csv` and `output/students.json`.

### Example interactive MongoDB searches

```text
Choose an option (1-4): 1
Enter Course ID: 103
# Expected: 36 students

Choose an option (1-4): 2
Enter Minimum GPA (0-4): 3.5
# Expected: 49 students

Choose an option (1-4): 3
Enter Course ID: 103
Enter Minimum GPA (0-4): 3.5
# Expected: 9 students
```

GPA input is validated within the inclusive range `0.0` to `4.0`; the search itself uses the **strict** `>` operator.

## Part 1 — Import and Indexing

The importer inserts all 100 student records into:

- **Database:** `university`
- **Collection:** `students`

Indexes:

| Index | Purpose |
|---|---|
| `courseIds_1` | Multikey index for searching within the `courseIds` array |
| `gpa_1` | Single-field index for GPA range queries |

The exporter writes **newline-delimited JSON** to `output/students.json`, one student document per line, for Spark SQL Option B.

## Parts 2–3 — MongoDB Queries

The interactive program supports:

- Students enrolled in a selected course, sorted by last name.
- Students with `gpa > minimumGpa`, sorted by GPA descending.
- Students satisfying **both** conditions using a combined filter.

The course-query `explain()` output confirmed use of the multikey index:

```text
stage: IXSCAN
indexName: courseIds_1
isMultiKey: true
nReturned: 36
totalKeysExamined: 36
totalDocsExamined: 36
```

**Interpretation:** MongoDB used the `courseIds_1` index to locate matching records rather than relying on a full collection scan for the course filter. The query may still perform additional work to sort the matches.

## Part 4 — Spark SQL Analysis

`SparkAnalysis.scala` reads `output/students.json` using `spark.read.json(...)`, converts the inferred types, and registers the temporary `students` view.

The normalized schema is:

```text
root
 |-- _id: integer (nullable = true)
 |-- firstName: string (nullable = true)
 |-- lastName: string (nullable = true)
 |-- dateOfBirth: date (nullable = true)
 |-- courseIds: array (nullable = true)
 |    |-- element: integer (containsNull = true)
 |-- gpa: double (nullable = true)
```

### 4.1 — Reproducing MongoDB Queries

The equivalent Spark SQL filters use `array_contains(courseIds, 103)` and `gpa > 3.5`.

| Query | MongoDB | Spark SQL | Match |
|---|---:|---:|---|
| Students in course 103 | 36 | 36 | Yes |
| Students with GPA > 3.5 | 49 | 49 | Yes |

The MongoDB combined query for course 103 and GPA > 3.5 returned **9** students.

### 4.2 — Arrays and Course Statistics

`explode(courseIds)` creates the `enrollments` view with one row per student–course enrollment (**218 rows**).

| Course | Students | Average GPA | Min GPA | Max GPA |
|---:|---:|---:|---:|---:|
| 101 | 48 | 3.56 | 3.1 | 3.9 |
| 102 | 43 | 3.50 | 3.1 | 3.9 |
| 103 | 36 | 3.39 | 3.1 | 3.9 |
| 104 | 44 | 3.59 | 3.1 | 3.9 |
| 105 | 47 | 3.50 | 3.1 | 3.9 |

Course load distribution:

| Courses per student | Number of students |
|---:|---:|
| 1 | 8 |
| 2 | 66 |
| 3 | 26 |

Checks: `8 + 66 + 26 = 100` students and `8×1 + 66×2 + 26×3 = 218` enrollments.

### 4.3 — Aggregations, Joins, and Window Functions

**Birth year summary:**

| Birth year | Students | Average GPA |
|---:|---:|---:|
| 1997 | 47 | 3.54 |
| 1998 | 29 | 3.50 |
| 1999 | 24 | 3.45 |

**Top 3 GPA ranks per course:** The query uses `DENSE_RANK() OVER (PARTITION BY course ORDER BY gpa DESC)` and keeps `gpaRank <= 3`.

**Handling ties:** Students with identical GPAs receive the same rank, and the next distinct GPA receives the next consecutive rank. Consequently, **94 student–course rows** qualify across the five courses, which is more than 3 students per course. This is intentional and preserves all ties.

**Matching names:** A self-join on both first and last name, with `s1._id < s2._id`, finds **7 pairs** and shows both student IDs and birth dates.

**Above every enrolled course average:** A join between enrollments and exact per-course GPA averages identifies students whose GPA is strictly higher than the average for **each** of their courses. **50 students** qualify. Exact (unrounded) averages are used for comparison.

### 4.4 — Saving and Verifying Results

The per-course summary is saved as Parquet to:

```text
output/course_stats/
```

Spark reads the Parquet directory back, displays all five course rows, and verifies that the reloaded data matches the original summary using row counts and bidirectional `exceptAll` checks. The run completed successfully.

> **Note:** `output/course_stats/` is a directory containing Spark-generated files, not a single `.parquet` file.

## MongoDB vs. Spark SQL

| Aspect | MongoDB | Spark SQL |
|---|---|---|
| Main role | Operational document storage and indexed lookups | Analytical processing across records |
| Data source here | `university.students` collection | Exported `output/students.json` |
| Typical operations | Array membership and numeric range filters | `explode`, `GROUP BY`, joins, window functions |
| Indexing | Uses `courseIds_1` multikey and `gpa_1` indexes | Queries the loaded DataFrame through Spark SQL |
| Verified equivalent filters | Course 103: 36; GPA > 3.5: 49 | Course 103: 36; GPA > 3.5: 49 |

Both systems produce matching results for the equivalent filtering questions. MongoDB is appropriate for indexed retrieval of specific student records; Spark SQL is more convenient for multi-step analysis and per-course summaries. This assignment does **not** benchmark query execution times, so it does not establish a measured speed advantage for either system.

## Verified Execution

The completed Spark SQL run reported:

```text
Total students: 100
Course 103 students: 36
Students with GPA > 3.5: 49
Total enrollments: 218
Total courses: 5
All Spark SQL tasks completed successfully!
Process finished with exit code 0
```

The MongoDB interactive menu was also tested successfully, including course filtering, GPA filtering, combined filtering, input validation, and normal exit.

## Submission Notes

Include the Scala source files, `build.sbt`, required sbt configuration, input CSV, and any requested evidence/output. Keep `output/students.json` available for Spark Option B. Include the Parquet output directory if generated results are part of the submission. IDE caches and compiled artifacts (`.idea/`, `.bsp/`, `target/`) can generally be excluded.
