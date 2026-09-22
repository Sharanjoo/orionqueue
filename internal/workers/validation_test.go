package workers

import "testing"

func validRegisterInput() RegisterInput {
	return RegisterInput{
		Hostname:            "worker-1.local",
		CPUCapacity:         8,
		MemoryCapacityBytes: 32 << 30,
		GPUs: []GPU{
			{DeviceIndex: 0, UUID: "GPU-fake-0", TotalMemoryBytes: 16 << 30, HealthState: GPUHealthy},
			{DeviceIndex: 1, UUID: "GPU-fake-1", TotalMemoryBytes: 16 << 30, HealthState: GPUHealthy},
		},
		SoftwareVersion: "0.1.0",
		Labels:          map[string]string{"env": "local"},
	}
}

func TestValidateRegisterAcceptsWellFormedInput(t *testing.T) {
	if err := ValidateRegister(validRegisterInput()); err != nil {
		t.Fatalf("expected valid input to pass, got: %v", err)
	}
}

func TestValidateRegisterRejectsEmptyHostname(t *testing.T) {
	in := validRegisterInput()
	in.Hostname = "  "
	if err := ValidateRegister(in); err == nil {
		t.Fatal("expected an error for empty hostname")
	}
}

func TestValidateRegisterRejectsNegativeCapacity(t *testing.T) {
	in := validRegisterInput()
	in.CPUCapacity = -1
	if err := ValidateRegister(in); err == nil {
		t.Fatal("expected an error for negative cpu_capacity")
	}

	in = validRegisterInput()
	in.MemoryCapacityBytes = -1
	if err := ValidateRegister(in); err == nil {
		t.Fatal("expected an error for negative memory_capacity_bytes")
	}
}

func TestValidateRegisterRejectsDuplicateDeviceIndex(t *testing.T) {
	in := validRegisterInput()
	in.GPUs[1].DeviceIndex = 0 // collides with GPUs[0]
	if err := ValidateRegister(in); err == nil {
		t.Fatal("expected an error for duplicate device_index")
	}
}

func TestValidateRegisterRejectsAllocatedExceedingTotal(t *testing.T) {
	in := validRegisterInput()
	in.GPUs[0].AllocatedMemoryBytes = in.GPUs[0].TotalMemoryBytes + 1
	if err := ValidateRegister(in); err == nil {
		t.Fatal("expected an error when allocated_memory_bytes exceeds total_memory_bytes")
	}
}

func TestValidateRegisterRejectsTooManyGPUs(t *testing.T) {
	in := validRegisterInput()
	in.GPUs = make([]GPU, MaxGPUCount+1)
	for i := range in.GPUs {
		in.GPUs[i] = GPU{DeviceIndex: int32(i), TotalMemoryBytes: 1}
	}
	if err := ValidateRegister(in); err == nil {
		t.Fatal("expected an error for exceeding MaxGPUCount")
	}
}

func TestValidateRegisterCollectsMultipleProblems(t *testing.T) {
	err := ValidateRegister(RegisterInput{CPUCapacity: -1, MemoryCapacityBytes: -1})
	if err == nil {
		t.Fatal("expected an error")
	}
	verr, ok := err.(*ValidationError)
	if !ok {
		t.Fatalf("expected *ValidationError, got %T", err)
	}
	if len(verr.Problems) < 3 {
		t.Errorf("expected at least 3 problems reported at once, got %d: %v", len(verr.Problems), verr.Problems)
	}
}
