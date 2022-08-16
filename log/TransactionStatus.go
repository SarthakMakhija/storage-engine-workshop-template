package log

type TransactionStatus struct {
	status string
}

const (
	successMarker = "@@@S@@@"
	failureMarker = "@@@F@@@"
)

func TransactionStatusSuccess() TransactionStatus {
	return TransactionStatus{status: successMarker}
}

func TransactionStatusFailed() TransactionStatus {
	return TransactionStatus{status: failureMarker}
}

func TransactionStatusFrom(bytes []byte) TransactionStatus {
	if string(bytes) == successMarker {
		return TransactionStatusSuccess()
	}
	return TransactionStatusFailed()
}

func TransactionStatusSize() uint8 {
	return uint8(len(successMarker))
}

func (transactionStatus TransactionStatus) Marshal() []byte {
	return []byte(transactionStatus.status)
}

func (transactionStatus TransactionStatus) isSuccess() bool {
	return transactionStatus.status == successMarker
}

func (transactionStatus TransactionStatus) isFailed() bool {
	return transactionStatus.status == failureMarker
}
