package log

import "testing"

func TestMarshalSuccessTransactionStatus(t *testing.T) {
	transactionStatusSuccess := TransactionStatusSuccess()
	bytes := transactionStatusSuccess.Marshal()

	status := TransactionStatusFrom(bytes)
	if !status.isSuccess() {
		t.Fatalf("Expected transaction status to be success, received %v", status.status)
	}
}

func TestMarshalFailedTransactionStatus(t *testing.T) {
	transactionStatusFailed := TransactionStatusFailed()
	bytes := transactionStatusFailed.Marshal()

	status := TransactionStatusFrom(bytes)
	if !status.isFailed() {
		t.Fatalf("Expected transaction status to be failed, received %v", status.status)
	}
}

func TestSuccessTransactionStatus(t *testing.T) {
	transactionStatusSuccess := TransactionStatusSuccess()
	isSuccess := transactionStatusSuccess.isSuccess()

	if !isSuccess {
		t.Fatalf("Expected transaction status to be success, received false")
	}
}

func TestFailureTransactionStatus(t *testing.T) {
	transactionStatusFailed := TransactionStatusFailed()
	isFailed := transactionStatusFailed.isFailed()

	if !isFailed {
		t.Fatalf("Expected transaction status to be failed, received false")
	}
}
