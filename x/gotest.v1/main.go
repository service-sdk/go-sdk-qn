package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"os/exec"
	"strconv"
)

func runAllTest(testPath string) []string {
	testCmd := exec.Command("go", "test", "-json", "-v", testPath)
	stdout, err := testCmd.StdoutPipe()
	if err != nil {
		panic(err)
	}
	go func() {
		_ = testCmd.Run()
	}()
	scanner := bufio.NewScanner(stdout)
	failedTests := make([]string, 0)
	for scanner.Scan() {
		line := scanner.Text()
		output := struct {
			Action string `json:"Action"`
			Test   string `json:"Test"`
		}{}
		err := json.Unmarshal([]byte(line), &output)
		if err != nil {
			log.Println("json unmarshal error:", err)
			log.Println("json line:", line)
		}
		if output.Action == "fail" && output.Test != "" {
			failedTests = append(failedTests, output.Test)
		}
	}
	return failedTests
}

func runSpecTest(testPath string, tests []string) (failedTest []string) {
	for _, test := range tests {
		fmt.Println("run test:", test)
		testCmd := exec.Command("go", "test", "-json", "-v", "-run", test, testPath)
		testCmd.Stdout = os.Stdout
		testCmd.Stderr = os.Stderr
		if err := testCmd.Run(); err != nil {
			// test failed
			failedTest = append(failedTest, test)
		}
	}
	return
}

func main() {
	args := os.Args
	if len(args) < 3 {
		fmt.Println("Usage: gotest <retry_count> <test_path>")
		os.Exit(1)
	}
	// 重试次数
	retryCount, err := strconv.ParseInt(args[1], 10, 64)
	if err != nil {
		fmt.Println("ParseInt error:", err)
		os.Exit(1)
	}

	// 测试包路径
	testPath := args[2]

	// 运行所有测试，记录失败的测试
	failedTests := runAllTest(testPath)

	// 重试失败的测试
	if len(failedTests) != 0 {
		for i := int64(0); i < retryCount; i++ {
			log.Printf("retry(%d): %v", i, failedTests)
			failedTests = runSpecTest(testPath, failedTests)

			// 如果没有失败的测试，退出
			if len(failedTests) == 0 {
				break
			}
		}
	}

	// 如果还有失败的测试，打印出来
	if len(failedTests) != 0 {
		log.Printf("failed tests: %v", failedTests)
		os.Exit(1)
	} else {
		log.Println("[ok] all tests passed")
	}
}
