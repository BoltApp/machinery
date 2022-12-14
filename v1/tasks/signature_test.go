package tasks

import (
	"fmt"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestConvertToSignature(t *testing.T) {
	type args struct {
		deprecatedSignature *DeprecatedSignature
	}
	defaultDeprecatedSignature := &DeprecatedSignature{

	}
	tests := []struct {
		name    string
		args    args
		want    *Signature
		wantErr assert.ErrorAssertionFunc
	}{
		{
			name: "Default",
			args: args{defaultDeprecatedSignature},
		}
	},
		for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := ConvertToSignature(tt.args.deprecatedSignature)
			if !tt.wantErr(t, err, fmt.Sprintf("ConvertToSignature(%v)", tt.args.deprecatedSignature)) {
				return
			}
			assert.Equalf(t, tt.want, got, "ConvertToSignature(%v)", tt.args.deprecatedSignature)
		})
	}
}
