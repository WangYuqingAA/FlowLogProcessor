# FlowLogProcessor Project

This project consists of three main components:

1. **TagRuleGenerator**: Generates random tag rules and writes them to a CSV file.
2. **FlowLogGenerator**: Generates synthetic flow log data and writes them to a CSV file.
3. **FlowLogProcessor**: Processes flow logs and tag rules, generating output CSV files with counts of unique port-protocol combinations and tag occurrences.

## Table of Contents

- [Project Structure](#project-structure)
- [Prerequisites](#prerequisites)
- [Installation](#installation)
- [Running the Project](#running-the-project)
- [Classes Overview](#classes-overview)
  - [TagRuleGenerator](#tagrulegenerator)
  - [FlowLogGenerator](#flowloggenerator)
  - [FlowLogProcessor](#flowlogprocessor)

## Project Structure

```
FlowLogProcessor/
├── src/
│   ├── TagRuleGenerator.java
│   ├── FlowLogGenerator.java
│   └── FlowLogProcessor.java
└── README.md
```

## Prerequisites

Before running this project, ensure you have the following software installed:

- **Java Development Kit (JDK) 8 or later**
- **Git** (for version control)

## Installation

### Clone the Repository

1. Clone the project to your local machine:

    ```
   git clone https://github.com/WangYuqingAA/FlowLogProcessor.git
   cd FlowLogProcessor
   ```

2. Compile the Java Files

   Compile the source files:

   ```bash
   javac -d bin src/*.java
   ```

## Running the Project

You can run the main program, which will generate flow logs, tag rules, and process them to create output files.

```bash
java -cp bin FlowLogProcessor
```

This will produce the following output files:

- `flow_logs.csv`: Generated flow logs.
- `tag_rules.csv`: Generated tag rules.
- `port_protocol_counts.csv`: Count of unique port-protocol combinations.
- `tag_counts.csv`: Count of tags based on flow logs and tag rules.

## Classes Overview

### TagRuleGenerator

This class generates random tag rules and writes them to a CSV file. The rules include various categories such as `SecurityGroup`, `Environment`, `Application`, and `Service`.

**Key Methods**:

- `generateTagRules(int numberOfRecords, String csvFile)`: Generates random tag rules and writes them to the specified CSV file.
- `randomPort()`: Generates a random port number with a weighted probability distribution.
- `randomProtocol()`: Selects a random protocol from a predefined list.
- `randomTag()`: Generates a random tag based on predefined categories and values.
- `writeToFile(String csvFile, String content)`: Writes the generated content to the specified CSV file.

### FlowLogGeneratoThis class generates synthetic flow log data and writes it to a CSV file. The generated flow logs include details such as IP addresses, ports, protocols, and actions.

**Key Methods**:

- `generateFlowLogs(int numberOfRecords, String csvFile)`: Generates a specified number of flow log records and writes them to a CSV file.
- `generateRandomIp()`: Generates a random private IPv4 address.
- `generateRandomPort()`: Generates a random port number with a realistic distribution.
- `generateSingleFlowLogRecord()`: Generates a single flow log record as a CSV string.

### FlowLogProcessor

This class processes flow logs and tag rules, generating CSV files with counts of unique port-protocol combinations and tag occurrences.

**Key Methods**:

- `parseFlowLogs(String csvFile)`: Parses the flow logs CSV file and returns a list of `DstAddrAndProtocol` objects.
- `parseTagRules(String csvFile)`: Parses the tag rules CSV file and returns a map of `DstAddrAndProtocol` to tags.
- `generatePortProtocolCountCsv(List<DstAddrAndProtocol> dstAddrAndProtocols, String outputCsvFile)`: Generates a CSV file with counts of unique port-protocol combinations.
- `generateTagCountCsv(List<DstAddrAndProtocol> dstAddrAndProtocols, Map<DstAddrAndProtocol, String> tagRuleMap, String outputCsvFile)`: Generates a CSV file with counts of tags based on flow logs and tag rules.


