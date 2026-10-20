package migrate

import (
	"strings"
	"testing"

	"gopkg.in/yaml.v3"
)

func TestApply_RetryPreservesUserData(t *testing.T) {
	for name, extra := range map[string]string{
		"variables": `
variables:
  payload:
    maxAttempt: 4
    maxAttempts: 9
    retry:
      type: constant
      maxAttempt: 7
`,
		"input defaults": `
inputs:
  - id: payload
    type: JSON
    defaults:
      maxAttempt: 4
      retry:
        maxAttempt: 7
`,
		"output value": `
outputs:
  - id: payload
    type: JSON
    value:
      maxAttempt: 4
`,
		"subflow inputs": `
tasks:
  - id: call
    type: io.kestra.plugin.core.flow.Subflow
    namespace: company.team
    flowId: child
    inputs:
      maxAttempt: 4
      retry:
        maxAttempt: 7
      tasks:
        - id: data
          type: io.kestra.plugin.core.log.Log
          retry:
            maxAttempt: 8
`,
		"trigger inputs": `
triggers:
  - id: schedule
    type: io.kestra.plugin.core.trigger.Schedule
    cron: "0 0 * * *"
    inputs:
      maxAttempt: 4
`,
		"task payload": `
tasks:
  - id: write
    type: io.kestra.plugin.core.kv.Set
    key: payload
    value:
      maxAttempt: 4
      retry:
        maxAttempt: 7
`,
		"custom plugin defaults property": `
tasks:
  - id: custom
    type: example.Custom
    defaults:
      - retry:
          maxAttempt: 7
`,
	} {
		t.Run(name, func(t *testing.T) {
			in := "id: retry-data\nnamespace: company.team\n" + extra
			if !strings.HasPrefix(extra, "\ntasks:") {
				in += "tasks:\n  - id: log\n    type: io.kestra.plugin.core.log.Log\n    message: \"{{ vars.payload.maxAttempt }}\"\n"
			}
			for _, v1 := range []bool{false, true} {
				var opts []Option
				if v1 {
					opts = append(opts, StayV1Compatible())
				}
				out, _, err := Apply([]byte(in), opts...)
				if err != nil {
					t.Fatal(err)
				}
				if string(out) != in {
					t.Errorf("stayV1=%v: user data changed:\n%s", v1, out)
				}
				var parsed map[string]any
				if err := yaml.Unmarshal(out, &parsed); err != nil {
					t.Errorf("stayV1=%v: invalid migrated mapping: %v", v1, err)
				}
			}
		})
	}
}

func TestApply_RetryPolicyLocations(t *testing.T) {
	task := "id: log\ntype: io.kestra.plugin.core.log.Log\nmessage: hello\nretry:\n  type: constant\n  interval: PT1S\n  maxAttempt: 3\n"
	indent := func(s string, spaces int) string {
		return strings.Repeat(" ", spaces) + strings.ReplaceAll(strings.TrimSuffix(s, "\n"), "\n", "\n"+strings.Repeat(" ", spaces)) + "\n"
	}
	taskList := func(s string) string {
		return "  - " + strings.ReplaceAll(strings.TrimSuffix(s, "\n"), "\n", "\n    ") + "\n"
	}
	for name, body := range map[string]string{
		"flow":                   "retry:\n  type: constant\n  interval: PT1S\n  maxAttempt: 3\ntasks:\n  - id: log\n    type: io.kestra.plugin.core.log.Log\n    message: hi\n",
		"task":                   "tasks:\n" + taskList(task),
		"errors":                 "errors:\n" + taskList(task),
		"finally":                "finally:\n" + taskList(task),
		"afterExecution":         "afterExecution:\n" + taskList(task),
		"listeners":              "listeners:\n  - tasks:\n" + indent(taskList(task), 4),
		"sequential":             "tasks:\n" + taskList("id: seq\ntype: io.kestra.plugin.core.flow.Sequential\ntasks:\n"+taskList(task)),
		"if branches":            "tasks:\n" + taskList("id: branch\ntype: io.kestra.plugin.core.flow.If\ncondition: true\nthen:\n"+taskList(task)+"else:\n"+taskList(task)),
		"switch cases":           "tasks:\n" + taskList("id: switch\ntype: io.kestra.plugin.core.flow.Switch\nvalue: x\ncases:\n  x:\n"+indent(taskList(task), 2)+"defaults:\n"+taskList(task)),
		"dag":                    "tasks:\n" + taskList("id: dag\ntype: io.kestra.plugin.core.flow.Dag\ntasks:\n  - task:\n"+indent(task, 6)),
		"pause":                  "tasks:\n" + taskList("id: pause\ntype: io.kestra.plugin.core.flow.Pause\nonPause:\n"+indent(task, 2)),
		"legacy if":              "tasks:\n" + taskList("id: branch\ntype: io.kestra.core.tasks.flows.If\ncondition: true\nthen:\n"+taskList(task)),
		"nested handlers":        "tasks:\n" + taskList("id: seq\ntype: io.kestra.plugin.core.flow.Sequential\nerrors:\n"+taskList(task)+"finally:\n"+taskList(task)),
		"plugin defaults":        "pluginDefaults:\n  - type: io.kestra.plugin.core.log.Log\n    values:\n      retry:\n        type: constant\n        maxAttempt: 3\n",
		"task defaults":          "taskDefaults:\n  - type: io.kestra.plugin.core.log.Log\n    values:\n      retry:\n        type: constant\n        maxAttempt: 3\n",
		"branch plugin defaults": "pluginDefaults:\n  - type: io.kestra.plugin.core.flow.If\n    values:\n      then:\n" + indent(taskList(task), 6),
	} {
		t.Run(name, func(t *testing.T) {
			in := "id: retry-locations\nnamespace: company.team\n" + body
			for _, v1 := range []bool{false, true} {
				var opts []Option
				if v1 {
					opts = append(opts, StayV1Compatible())
				}
				out, _, err := Apply([]byte(in), opts...)
				if err != nil {
					t.Fatal(err)
				}
				if strings.Contains(string(out), "maxAttempt:") || !strings.Contains(string(out), "maxAttempts: 3") {
					t.Errorf("stayV1=%v: retry was not migrated:\n%s", v1, out)
				}
				again, _, err := Apply(out, opts...)
				if err != nil {
					t.Fatal(err)
				}
				if string(again) != string(out) {
					t.Errorf("stayV1=%v: migration is not idempotent", v1)
				}
			}
		})
	}
}

func TestApply_RetryPolicyAlongsideUserData(t *testing.T) {
	in := `id: mixed-retry
namespace: company.team
variables:
  maxAttempt: 4
tasks:
  - id: call
    type: io.kestra.plugin.core.flow.Subflow
    namespace: company.team
    flowId: child
    retry:
      type: constant
      interval: PT1S
      maxAttempt: 3
    inputs:
      maxAttempt: "{{ vars.maxAttempt }}"
      retry:
        maxAttempt: 7
pluginDefaults:
  - type: io.kestra.plugin.core.flow.Subflow
    values:
      retry:
        type: constant
        maxAttempt: 3
      inputs:
        maxAttempt: 4
`
	want := strings.ReplaceAll(in, "maxAttempt: 3", "maxAttempts: 3")
	if out := apply(t, in); out != want {
		t.Errorf("got:\n%s\nwant:\n%s", out, want)
	}
}
