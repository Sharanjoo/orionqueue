package jobs

import "testing"

func validSubmitInput() SubmitInput {
	return SubmitInput{
		Name:  "train-resnet",
		Owner: "sharan",
		Image: "orionqueue/fake-gpu-job:latest",
		Resources: ResourceRequest{
			GPUCount:          1,
			MinGPUMemoryBytes: 8 << 30,
			CPUCores:          2,
			MemoryBytes:       4 << 30,
		},
		Priority:   50,
		RetryLimit: 3,
	}
}

func TestValidateSubmitAcceptsWellFormedInput(t *testing.T) {
	if err := ValidateSubmit(validSubmitInput()); err != nil {
		t.Fatalf("expected valid input to pass, got: %v", err)
	}
}

func TestValidateSubmitRejectsEmptyName(t *testing.T) {
	in := validSubmitInput()
	in.Name = "  "
	err := ValidateSubmit(in)
	if err == nil {
		t.Fatal("expected an error for empty name")
	}
	var verr *ValidationError
	if !isValidationError(err, &verr) {
		t.Fatalf("expected *ValidationError, got %T", err)
	}
}

func TestValidateSubmitRejectsEmptyOwner(t *testing.T) {
	in := validSubmitInput()
	in.Owner = ""
	if err := ValidateSubmit(in); err == nil {
		t.Fatal("expected an error for empty owner")
	}
}

func TestValidateSubmitRejectsEmptyImage(t *testing.T) {
	in := validSubmitInput()
	in.Image = ""
	if err := ValidateSubmit(in); err == nil {
		t.Fatal("expected an error for empty image")
	}
}

func TestValidateSubmitRejectsExcessiveGPUCount(t *testing.T) {
	in := validSubmitInput()
	in.Resources.GPUCount = MaxGPUCount + 1
	if err := ValidateSubmit(in); err == nil {
		t.Fatal("expected an error for gpu_count over the limit")
	}
}

func TestValidateSubmitRejectsNegativeResources(t *testing.T) {
	cases := map[string]SubmitInput{
		"negative min gpu memory": func() SubmitInput {
			in := validSubmitInput()
			in.Resources.MinGPUMemoryBytes = -1
			return in
		}(),
		"negative cpu cores": func() SubmitInput {
			in := validSubmitInput()
			in.Resources.CPUCores = -1
			return in
		}(),
		"negative memory bytes": func() SubmitInput {
			in := validSubmitInput()
			in.Resources.MemoryBytes = -1
			return in
		}(),
	}
	for name, in := range cases {
		t.Run(name, func(t *testing.T) {
			if err := ValidateSubmit(in); err == nil {
				t.Fatalf("expected an error for %s", name)
			}
		})
	}
}

func TestValidateSubmitRejectsOutOfRangePriority(t *testing.T) {
	for _, p := range []int32{-1, MaxPriority + 1} {
		in := validSubmitInput()
		in.Priority = p
		if err := ValidateSubmit(in); err == nil {
			t.Fatalf("expected an error for priority %d", p)
		}
	}
}

func TestValidateSubmitRejectsNegativeRetryLimit(t *testing.T) {
	in := validSubmitInput()
	in.RetryLimit = -1
	if err := ValidateSubmit(in); err == nil {
		t.Fatal("expected an error for negative retry_limit")
	}
}

func TestValidateSubmitRejectsExcessiveRetryLimit(t *testing.T) {
	in := validSubmitInput()
	in.RetryLimit = MaxRetryLimit + 1
	if err := ValidateSubmit(in); err == nil {
		t.Fatal("expected an error for retry_limit over the limit")
	}
}

func TestValidateSubmitRejectsNegativeTimeouts(t *testing.T) {
	in := validSubmitInput()
	in.TimeoutSeconds = -1
	if err := ValidateSubmit(in); err == nil {
		t.Fatal("expected an error for negative timeout_seconds")
	}

	in = validSubmitInput()
	in.CheckpointIntervalSeconds = -1
	if err := ValidateSubmit(in); err == nil {
		t.Fatal("expected an error for negative checkpoint_interval_seconds")
	}
}

func TestValidateSubmitCollectsMultipleProblems(t *testing.T) {
	in := SubmitInput{} // empty: name, owner, image all missing
	err := ValidateSubmit(in)
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

func isValidationError(err error, target **ValidationError) bool {
	verr, ok := err.(*ValidationError)
	if ok {
		*target = verr
	}
	return ok
}
