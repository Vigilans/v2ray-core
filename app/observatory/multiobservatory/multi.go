package multiobservatory

import (
	"context"

	"github.com/golang/protobuf/jsonpb"
	"github.com/golang/protobuf/proto"

	"github.com/v2fly/v2ray-core/v5/app/observatory"
	"github.com/v2fly/v2ray-core/v5/common"
	"github.com/v2fly/v2ray-core/v5/common/taggedfeatures"
	"github.com/v2fly/v2ray-core/v5/features"
	"github.com/v2fly/v2ray-core/v5/features/extension"
)

type Observer struct {
	features.TaggedFeatures
	config *Config
	ctx    context.Context
}

func (o Observer) GetObservation(ctx context.Context) (proto.Message, error) {
	tags, err := o.GetFeaturesTag()
	if err != nil {
		return nil, err
	}
	results := &observatory.ObservationResult{}
	for _, tag := range tags {
		feature, err := o.GetFeaturesByTag(tag)
		if err != nil {
			return nil, err
		}
		observeResult, err := feature.(extension.Observatory).GetObservation(ctx)
		if err != nil {
			return nil, err
		}
		results.Status = append(results.Status, observeResult.(*observatory.ObservationResult).Status...)
	}
	return results, nil
}

func (o Observer) Type() interface{} {
	return extension.ObservatoryType()
}

func New(ctx context.Context, config *Config) (*Observer, error) {
	holder, err := taggedfeatures.NewHolderFromConfig(ctx, config.Holders, extension.ObservatoryType())
	if err != nil {
		return nil, err
	}
	return &Observer{config: config, ctx: ctx, TaggedFeatures: holder}, nil
}

func (x *Config) UnmarshalJSONPB(unmarshaler *jsonpb.Unmarshaler, bytes []byte) error {
	var err error
	x.Holders, err = taggedfeatures.LoadJSONConfig(context.TODO(), "service", "background", bytes)
	return err
}

func init() {
	common.Must(common.RegisterConfig((*Config)(nil), func(ctx context.Context, config interface{}) (interface{}, error) {
		return New(ctx, config.(*Config))
	}))
}
